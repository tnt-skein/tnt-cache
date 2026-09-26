--- Живая проверка драйвера на Redis: настоящий сервер стенда.
---
--- Двойник клиента показывает, какие команды драйвер шлёт. Что сервер
--- с ними делает — срок `PXAT`, `SET NX`, `GETDEL`, сценарий прибавки
--- с `struct` и `KEEPTTL`, `SCAN` с образцом, — видно только здесь.
--- Сервер поднимается отдельно — `make redis-up` (`test/stand/redis.sh`);
--- нет его — проверки пропускаются: гейты от докера не зависят.

local decimal = require('decimal')
local fiber = require('fiber')
local t = require('luatest')
local uuid = require('uuid')

local helper = dofile('test/helper.lua')

local redis = helper.redis()

local g = t.group('tnt.cache.redis_live')

--- Окружение стенда — через `tnt-env`: мимо него окружение не читают
--- и проверки.
local env = helper.stand_env()

--- Где стоит сервер: те же переменные, что у скрипта стенда.
local PORT = env.int('STAND_REDIS_PORT', 16379)
local PASSWORD = 'stand-secret'

---@type any
local cache

g.before_all(function()
    t.skip_if(
        not helper.listening('127.0.0.1', PORT),
        'Redis не отвечает: поднимите его — make redis-up'
    )

    g.client = redis.new({ port = PORT, password = PASSWORD, pool = { sweep_interval = 0 } })
end)

g.after_all(function()
    if g.client ~= nil then
        g.client:close()
    end
end)

g.before_each(function()
    cache = helper.load()
    helper.journal.forget()

    -- Своё пространство на проверку: ключи прошлого прогона, упавшего
    -- на середине, этому не мешают, а flush соседа не задевает.
    g.namespace = ('cache-live:%s:'):format(uuid.str())
    g.store = cache.new({ driver = 'redis', redis = g.client, namespace = g.namespace })
end)

g.after_each(function()
    g.store:flush()
    helper.unload()
end)

--- Команда серверу мимо кэша.
---@param args any[]
---@return any
local function server(args)
    local value, err = g.client:command(args)

    t.assert_equals(err, nil)

    return value
end

--- Остаток срока ключа кэша в миллисекундах: -1 — навсегда, -2 — нет.
---@param key string
---@return integer
local function pttl(key)
    return server({ 'PTTL', g.namespace .. key })
end

g.test_values_go_there_and_back_with_the_server_keeping_the_ttl = function()
    local store = g.store
    local value = {
        id = 7,
        name = 'Мария',
        tags = { 'a', 'b' },
        big = 9007199254740993LL,
        price = decimal.new('1.10'),
    }

    store:put('card', value, 60)
    store:put('forever', 'навсегда')
    store:put('number', 42)

    t.assert_equals(store:get('card'), value)
    t.assert_equals(store:get('forever'), 'навсегда')
    t.assert_equals(store:get('number'), 42)
    t.assert_equals(store:has('card'), true)
    t.assert_equals(store:get('none', 'нет'), 'нет')
    t.assert_equals(store:has('none'), false)
    t.assert_almost_equals(pttl('card'), 60000, 1000, 'срок у ключа — у самого сервера')
    t.assert_equals(pttl('forever'), -1, 'навсегда — без срока')

    store:forget('card')

    t.assert_equals(store:get('card'), nil)
    t.assert_equals(pttl('card'), -2, 'стёрто на сервере')
end

g.test_the_server_erases_the_expired_itself = function()
    local store = g.store

    store:put('short', 1, 0.2)

    t.assert_equals(store:get('short'), 1)

    fiber.sleep(0.3)

    t.assert_equals(store:get('short'), nil)
    t.assert_equals(pttl('short'), -2, 'стёрто сервером, без sweep')
    t.assert_equals(store:sweep(), { swept = 0, scanned = 0 }, 'обходить нечего')
end

-- add — один SET NX: замок достаётся одному, а просроченный освобождается
-- сервером.
g.test_add_takes_the_lock_once = function()
    local store = g.store
    local taken = 0
    local fibers = {}

    for index = 1, 5 do
        local worker = fiber.new(function()
            if store:add('lock', index, 0.2) then
                taken = taken + 1
            end
        end)

        worker:set_joinable(true)
        table.insert(fibers, worker)
    end

    for _, worker in ipairs(fibers) do
        worker:join()
    end

    t.assert_equals(taken, 1, 'замок у одного из пяти')

    fiber.sleep(0.3)

    t.assert_equals(store:add('lock', 'снова'), true, 'просроченный замок свободен')
    t.assert_equals(store:get('lock'), 'снова')
    t.assert_equals(pttl('lock'), -1)
end

-- pull — один GETDEL: одноразовый знак достаётся ровно одному.
g.test_pull_hands_the_token_out_once = function()
    local store = g.store

    store:put('token', 'знак', 60)

    local got = {}
    local fibers = {}

    for index = 1, 5 do
        local worker = fiber.new(function()
            got[index] = store:pull('token', false)
        end)

        worker:set_joinable(true)
        table.insert(fibers, worker)
    end

    for _, worker in ipairs(fibers) do
        worker:join()
    end

    table.sort(got, function(left, right)
        return tostring(left) < tostring(right)
    end)

    t.assert_equals(got, { false, false, false, false, 'знак' })
    t.assert_equals(pttl('token'), -2)
end

-- Прибавка — сценарий на сервере: срок живой записи держится, новой —
-- ставится, дробное и отрицательное считаются точно, прибавки соседних
-- файберов не теряются.
g.test_increment_runs_on_the_server = function()
    local store = g.store
    local ttl = cache.new({
        driver = 'redis',
        redis = g.client,
        namespace = g.namespace,
        ttl = 30,
    })

    t.assert_equals(store:increment('hits'), 1)
    t.assert_equals(store:increment('hits', 5), 6)
    t.assert_equals(store:decrement('hits', 2), 4)
    t.assert_equals(store:decrement('hits'), 3)
    t.assert_equals(pttl('hits'), -1, 'новая без срока по умолчанию — навсегда')

    t.assert_equals(ttl:increment('fresh', 2), 2)
    t.assert_almost_equals(pttl('fresh'), 30000, 1000, 'новой — срок по умолчанию')

    store:put('kept', 10, 100)

    t.assert_equals(ttl:increment('kept', 0.5), 10.5)
    t.assert_almost_equals(pttl('kept'), 100000, 1000, 'срок живой записи держится')

    store:put('fraction', 0.1)

    t.assert_equals(store:increment('fraction', 0.2), 0.1 + 0.2)
    t.assert_equals(store:get('fraction'), 0.1 + 0.2)

    store:put('big', 2 ^ 63)

    t.assert_equals(store:increment('big', 0), 2 ^ 63, 'за 2^63 — без смены знака')

    local fibers = {}

    for _ = 1, 20 do
        local worker = fiber.new(function()
            store:increment('race')
        end)

        worker:set_joinable(true)
        table.insert(fibers, worker)
    end

    for _, worker in ipairs(fibers) do
        worker:join()
    end

    t.assert_equals(store:get('race'), 20, 'ни одна прибавка не потеряна')
end

-- Не число — не прибавляется, и значение остаётся цело: строка, целое
-- cdata, пустая строка и чужой double короче своего.
g.test_increment_refuses_what_is_not_a_number = function()
    local store = g.store

    store:put('word', 'слово')
    store:put('cdata', 5LL)
    server({ 'SET', g.namespace .. 'empty', '' })

    t.assert_error_msg_equals(
        'кэш: word — не число, прибавлять нечего',
        store.increment,
        store,
        'word'
    )
    t.assert_error_msg_equals(
        'кэш: cdata — не число, прибавлять нечего',
        store.increment,
        store,
        'cdata'
    )
    t.assert_error_msg_equals(
        'кэш: empty — не число, прибавлять нечего',
        store.increment,
        store,
        'empty'
    )
    t.assert_equals(store:get('word'), 'слово')
    t.assert_equals(store:get('cdata'), 5)

    server({ 'SET', g.namespace .. 'short', '\xcb\x00' })

    local ok, err = pcall(store.increment, store, 'short')

    t.assert_equals(ok, false)
    t.assert_equals(err.kind, 'rejected', 'чужой обрывок double — отказ сервера')
end

-- Чужое под своим ключом — промах с записью в журнале.
g.test_foreign_payload_reads_as_a_miss = function()
    local store = g.store

    server({ 'SET', g.namespace .. 'foreign', 'abc' })

    t.assert_equals(store:get('foreign', 'нет'), 'нет')
    t.assert_equals(helper.journal.records()[1].record.fields, {
        driver = 'redis',
        err = 'кэш: под ключом в Redis лежит не msgpack — запись не кэша',
    })
end

-- flush стирает своё пространство целиком — кусками SCAN, — а чужие
-- ключи базы, и похожие на свои по образцу, не трогает.
g.test_flush_erases_only_its_namespace = function()
    local store = g.store
    local commands = {}

    for index = 1, 2500 do
        table.insert(commands, { 'SET', ('%sk%d'):format(g.namespace, index), 1 })
    end

    local _, err = g.client:pipeline(commands)

    t.assert_equals(err, nil)

    local stranger = g.namespace:gsub(':$', 'x:') .. 'k1'

    server({ 'SET', stranger, 'чужое' })

    store:flush()

    local keys = server({ 'SCAN', '0', 'MATCH', g.namespace .. '*', 'COUNT', 10000 })

    t.assert_equals(keys[2], {})
    t.assert_equals(server({ 'GET', stranger }), 'чужое', 'чужой ключ цел')

    server({ 'DEL', stranger })

    -- Без экранирования образец `…:?*` поймал бы и `…:b` соседа.
    local odd = cache.new({ driver = 'redis', redis = g.client, namespace = g.namespace .. '?' })

    odd:put('a', 1)
    store:put('b', 2)
    odd:flush()

    t.assert_equals(odd:get('a'), nil)
    t.assert_equals(store:get('b'), 2, 'знаки глоба в пространстве — буквально')
end

-- Сервер недоступен: чтение — промах с записью в журнале, запись —
-- бросок самого отказа с родом.
g.test_unreachable_server_keeps_the_failure_contract = function()
    local dead = redis.new({
        port = 1,
        timeout = 0.3,
        pool = { wait_timeout = 0.1, sweep_interval = 0 },
    })
    local store = cache.new({ driver = 'redis', redis = dead })

    t.assert_equals(store:get('a', 'нет'), 'нет')

    -- В журнале и запись пула о неоткрытом соединении: кэшу — своя.
    local own = {}

    for _, record in ipairs(helper.journal.records()) do
        if record.module == 'tnt.cache' then
            table.insert(own, record.record)
        end
    end

    t.assert_equals(#own, 1)
    t.assert_equals(own[1].message, 'кэш: драйвер redis отказал на чтении — промах')
    t.assert_equals(own[1].fields.driver, 'redis')
    t.assert_str_contains(own[1].fields.err, 'соединение не открылось')

    local ok, err = pcall(store.put, store, 'a', 1)

    t.assert_equals(ok, false)
    t.assert_equals(err.kind, 'unreachable', 'запись бросает сам отказ')

    dead:close()
end

-- ── Теги и замок ─────────────────────────────────────────────────────

--- Ключ множества тега на сервере.
---@param tag string
---@return string
local function tag_set(tag)
    return g.namespace .. '\0tag:' .. tag
end

-- Две записи с общим тегом забываются разом, третья с другим тегом
-- и четвёртая без тегов — целы; множество тега уходит вместе с ними.
g.test_tag_flush_erases_both_and_leaves_the_third = function()
    local store = g.store

    store:tags('docs'):put('docs:1', 'первая', 60)
    store:tags({ 'lists', 'docs' }):put('docs:index', { 1, 2 })
    store:tags('news'):put('news:1', 'новость')
    store:put('plain', 'без тегов')

    t.assert_equals(store:get('docs:index'), { 1, 2 })
    t.assert_equals(store:tags('docs'):flush(), 2)
    t.assert_equals({ store:get('docs:1'), store:get('docs:index') }, {})
    t.assert_equals(store:get('news:1'), 'новость')
    t.assert_equals(store:get('plain'), 'без тегов')
    t.assert_equals(server({ 'EXISTS', tag_set('docs') }), 0, 'множество забытого тега ушло')
    t.assert_equals(
        store:tags('lists'):flush(),
        0,
        'во втором теге ключ ждал срока — стирать нечего'
    )
end

-- Множество тега вычищает ключи, чей срок вышел, и живёт до срока
-- последнего; с вечным ключом — вечно.
g.test_tag_sets_trim_the_dead_and_live_as_long_as_the_last = function()
    local store = g.store
    local now = assert(tonumber(server({ 'TIME' })[1]))
    local ghost = g.namespace .. 'ghost'

    server({ 'ZADD', tag_set('docs'), (now - 3600) * 1000, ghost })
    store:tags('docs'):put('a', 1, 60)

    t.assert_equals(
        server({ 'ZRANGE', tag_set('docs'), 0, -1 }),
        { g.namespace .. 'a' },
        'мёртвый вычищен'
    )
    t.assert_almost_equals(server({ 'PTTL', tag_set('docs') }), 60000, 1000)

    store:tags({ 'docs', 'lists' }):put('b', 2, 120)
    store:tags('docs'):put('c', 3, 30)

    t.assert_almost_equals(
        server({ 'PTTL', tag_set('docs') }),
        120000,
        1000,
        'срок — у последнего ключа'
    )
    t.assert_almost_equals(server({ 'PTTL', tag_set('lists') }), 120000, 1000)
    t.assert_equals(server({ 'ZCARD', tag_set('docs') }), 3, 'живые остались')

    store:tags('docs'):put('d', 4)

    t.assert_equals(
        server({ 'PTTL', tag_set('docs') }),
        -1,
        'с вечным ключом множество вечно'
    )
    t.assert_equals(server({ 'ZSCORE', tag_set('docs'), g.namespace .. 'd' }), 'inf')
end

-- add с тегами — одно действие: замок достаётся одному и помечен тегом.
g.test_tagged_add_takes_once_and_marks = function()
    local store = g.store
    local docs = store:tags('docs')

    t.assert_equals(docs:add('a', 'первый', 60), true)
    t.assert_equals(docs:add('a', 'второй', 60), false)
    t.assert_equals(store:get('a'), 'первый')
    t.assert_equals(server({ 'ZCARD', tag_set('docs') }), 1)
    t.assert_equals(docs:flush(), 1)
end

-- Кусок забывания не длиннее предела; ключ, стёртый мимо тега, взят,
-- но не стёрт; пустой тег — ноль.
g.test_forget_tag_takes_chunks = function()
    local store = g.store

    for index = 1, 3 do
        store:tags('docs'):put('k' .. index, index)
    end

    store:forget('k3')

    t.assert_equals({ store.driver.erase_tagged('docs', 2) }, { 2, 2 })
    t.assert_equals({ store.driver.erase_tagged('docs', 2) }, { 1, 0 })
    t.assert_equals({ store.driver.erase_tagged('docs', 2) }, { 0, 0 })

    store:tags('docs'):put('k1', 1)

    t.assert_equals({ store.driver.erase_tagged('docs', 1) }, { 1, 1 })
    t.assert_equals(store:get('k1'), nil)
end

-- Снимает замок только держатель: метка соседа под тем же ключом цела.
g.test_release_only_its_own_lock = function()
    local store = g.store
    local lock = store:lock('report', 60)
    local key = g.namespace .. '\0lock:report'

    t.assert_equals({ store:lock('report', 60) }, { nil, 'кэш: замок report занят' })
    t.assert_almost_equals(server({ 'PTTL', key }), 60000, 1000)

    server({ 'SET', key, require('msgpack').encode('соседа') })

    t.assert_equals(lock:release(), false)
    t.assert_equals(server({ 'EXISTS', key }), 1, 'замок соседа цел')

    server({ 'SET', key, require('msgpack').encode(lock.token) })

    t.assert_equals(lock:release(), true)
    t.assert_equals(server({ 'EXISTS', key }), 0)
end

-- Десять промахов разом на одном ключе — один счёт.
g.test_remember_with_a_lock_computes_once = function()
    local calls, got = helper.stampede(g.store, 'page')

    t.assert_equals({ calls, #got, got[1], got[10] }, { 1, 10, 'страница', 'страница' })
end
