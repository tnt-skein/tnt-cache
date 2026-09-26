--- Тесты драйвера на Redis без сервера: клиент tnt-redis — двойник,
--- и видно каждую посланную команду с её настройками вызова.
---
--- Поведение настоящего сервера — `PXAT`, `SET NX`, `GETDEL`, сценарий
--- прибавки, `SCAN` с образцом — проверяет `redis_live_test.lua`.

local ffi = require('ffi')
local msgpack = require('msgpack')
local t = require('luatest')

local helper = dofile('test/helper.lua')

local g = t.group('tnt.cache.redis')

---@type any
local cache

g.before_each(function()
    cache = helper.load()
    helper.journal.forget()
end)

g.after_each(helper.unload)

--- Повторяемое после отправки: чтение, запись того же, стирание.
local REPEATABLE = { idempotent = true }

--- Число в строке байтов — всегда msgpack double.
---@param number number
---@return string
local function double(number)
    return msgpack.encode(ffi.cast('double', number))
end

--- Драйвер над двойником клиента.
---@param replies table[]
---@param opts table|nil
---@return table driver
---@return table client
local function driver(replies, opts)
    local client = helper.fake_redis(replies)

    return cache.redis.new(client, opts), client
end

-- ── Сборка ───────────────────────────────────────────────────────────

g.test_driver_needs_a_client_and_a_namespace = function()
    local no_client = 'кэш: драйверу redis нужен клиент tnt-redis — аргумент redis'
    local no_namespace =
        'кэш: namespace драйвера redis — непустая строка: без неё flush стёр бы чужие ключи базы'

    t.assert_error_msg_equals(no_client, cache.redis.new, nil)
    t.assert_error_msg_equals(no_client, cache.redis.new, 'клиент')
    t.assert_error_msg_equals(no_client, cache.redis.new, {})
    t.assert_error_msg_equals(no_client, cache.redis.new, { command = 'GET' })
    t.assert_error_msg_equals(
        no_client,
        cache.new,
        { driver = 'redis' },
        'без клиента и через cache.new'
    )
    t.assert_error_msg_equals(no_namespace, cache.redis.new, helper.fake_redis({}), { namespace = '' })
    t.assert_error_msg_equals(no_namespace, cache.redis.new, helper.fake_redis({}), { namespace = 7 })

    local built = driver({})

    t.assert_equals(built.name, 'redis')
    t.assert_equals(built.remote, true, 'драйвер по сети')
    t.assert_equals(cache.redis.NAMESPACE, 'cache:')
end

-- ── Чтение и запись ──────────────────────────────────────────────────

-- Значение уходит msgpack в строке байтов под пространством ключей;
-- срок — мигом PXAT в миллисекундах, вверх; навсегда — без него.
g.test_writes_msgpack_under_the_namespace_with_the_expiry_moment = function()
    local d, client = driver({ { 'OK' }, { 'OK' }, { 'OK' } })

    d.write('a', { id = 7 }, cache.store.FOREVER)
    d.write('n', 5, 1000.0004)
    d.write('m', 'мигом', 2000)

    t.assert_equals(client.calls, {
        { args = { 'SET', 'cache:a', msgpack.encode({ id = 7 }) }, opts = REPEATABLE },
        { args = { 'SET', 'cache:n', double(5), 'PXAT', 1000001 }, opts = REPEATABLE },
        { args = { 'SET', 'cache:m', msgpack.encode('мигом'), 'PXAT', 2000000 }, opts = REPEATABLE },
    })
    t.assert_equals(
        { client.calls[2].args[3]:byte(1), #client.calls[2].args[3] },
        { 0xcb, 9 },
        'число — всегда double: прибавка на сервере читает только его'
    )
end

-- Срок живого решает сервер: прочитанное — живо, срок 0.
g.test_reads_the_value_back_as_alive = function()
    local d, client = driver({
        { msgpack.encode({ id = 7, tags = { 'a' } }) },
        {},
        { double(0.5) },
    }, { namespace = 'app:' })

    t.assert_equals({ d.read('a') }, { { id = 7, tags = { 'a' } }, 0 })
    t.assert_equals({ d.read('b') }, {}, 'промах — пусто')
    t.assert_equals({ d.read('n') }, { 0.5, 0 })
    t.assert_equals(client.calls, {
        { args = { 'GET', 'app:a' }, opts = REPEATABLE },
        { args = { 'GET', 'app:b' }, opts = REPEATABLE },
        { args = { 'GET', 'app:n' }, opts = REPEATABLE },
    })
end

-- Хвост за значением либо не msgpack — не запись кэша: бросок, а
-- хранилище сделает из него промах.
g.test_refuses_a_payload_that_is_not_its_own = function()
    local d = driver({ { 'abc' }, { '\xc1' }, { msgpack.encode(7) .. 'x' } })

    t.assert_error_msg_equals(
        'кэш: под ключом в Redis лежит не msgpack — запись не кэша',
        d.read,
        'a'
    )
    t.assert_error_msg_contains('Invalid MsgPack', d.read, 'b')
    t.assert_error_msg_equals(
        'кэш: под ключом в Redis лежит не msgpack — запись не кэша',
        d.read,
        'c'
    )
end

-- Отказ клиента бросается как есть: род и приговор доходят до того,
-- кто поймает его на границе запроса.
g.test_throws_the_client_failure_itself = function()
    local failure =
        { kind = 'unreachable', message = 'redis 127.0.0.1:1: соединение не открылось' }
    local d = driver({ { nil, failure }, { nil, failure } })

    local ok, err = pcall(d.read, 'a')

    t.assert_equals(ok, false)
    t.assert_is(err, failure)

    ok, err = pcall(d.write, 'a', 1, 0)

    t.assert_equals(ok, false)
    t.assert_is(err, failure)
end

-- ── Одним действием ──────────────────────────────────────────────────

-- То, что пишет по прочитанному, — одна команда сервера и без повтора
-- после отправки: повтор решил бы заново то, что сервер уже решил.
g.test_add_take_and_increment_are_single_commands = function()
    local d, client = driver({
        { 'OK' },
        {},
        { 'OK' },
        { msgpack.encode('знак') },
        {},
        { double(7) },
        { double(-1) },
        { '' },
    })

    t.assert_equals(d.add('lock', true, 1000.5), true)
    t.assert_equals(d.add('lock', true, 1000.5), false, 'занято — не положено')
    t.assert_equals(d.add('flag', 'навсегда', 0), true)
    t.assert_equals({ d.take('token') }, { 'знак', 0 })
    t.assert_equals({ d.take('token') }, {}, 'забранное второй раз — промах')
    t.assert_equals(d.increment('hits', 2, 1000.5), 7)
    t.assert_equals(d.increment('left', -1, 0), -1)
    t.assert_equals(d.increment('word', 1, 0), nil, 'не число — пусто')

    local lock = msgpack.encode(true)

    t.assert_equals(client.calls, {
        { args = { 'SET', 'cache:lock', lock, 'NX', 'PXAT', 1000500 } },
        { args = { 'SET', 'cache:lock', lock, 'NX', 'PXAT', 1000500 } },
        { args = { 'SET', 'cache:flag', msgpack.encode('навсегда'), 'NX' } },
        { args = { 'GETDEL', 'cache:token' } },
        { args = { 'GETDEL', 'cache:token' } },
        { args = { 'EVAL', cache.redis.INCREMENT, 1, 'cache:hits', 2, 1000500 } },
        { args = { 'EVAL', cache.redis.INCREMENT, 1, 'cache:left', -1 } },
        { args = { 'EVAL', cache.redis.INCREMENT, 1, 'cache:word', 1 } },
    })
end

-- ── Стирание ─────────────────────────────────────────────────────────

g.test_erases_one_and_a_batch_under_the_namespace = function()
    local d, client = driver({ { 1 }, { 2 } })

    d.erase('a')
    d.erase_many({ 'b', 'c' })

    t.assert_equals(client.calls, {
        { args = { 'DEL', 'cache:a' }, opts = REPEATABLE },
        { args = { 'DEL', 'cache:b', 'cache:c' }, opts = REPEATABLE },
    })
end

-- flush стирает только своё пространство: обход SCAN с образцом, где
-- знаки глоба в пространстве — буквально; пустой кусок DEL не шлёт.
g.test_clear_scans_its_namespace_and_deletes_by_the_chunk = function()
    local d, client = driver({
        { { '17', { 'a*b[?]\\:1', 'a*b[?]\\:2' } } },
        { 2 },
        { { '40', {} } },
        { { '0', { 'a*b[?]\\:3' } } },
        { 1 },
    }, { namespace = 'a*b[?]\\:' })

    d.clear()

    local pattern = 'a\\*b\\[\\?\\]\\\\:*'

    t.assert_equals(client.calls, {
        { args = { 'SCAN', '0', 'MATCH', pattern, 'COUNT', 1000 }, opts = REPEATABLE },
        { args = { 'DEL', 'a*b[?]\\:1', 'a*b[?]\\:2' }, opts = REPEATABLE },
        { args = { 'SCAN', '17', 'MATCH', pattern, 'COUNT', 1000 }, opts = REPEATABLE },
        { args = { 'SCAN', '40', 'MATCH', pattern, 'COUNT', 1000 }, opts = REPEATABLE },
        { args = { 'DEL', 'a*b[?]\\:3' }, opts = REPEATABLE },
    })
end

-- Просроченное Redis стирает сам: обходить нечего, и sweep не шлёт
-- ни одной команды.
g.test_sweep_has_nothing_to_walk = function()
    local client = helper.fake_redis({})
    local store = cache.new({ driver = 'redis', redis = client, now = helper.clock().realtime })

    t.assert_equals({ store.driver.scan(nil, 1000) }, { {} })
    t.assert_equals(store:sweep(), { swept = 0, scanned = 0 })
    t.assert_equals(client.calls, {})
end

-- ── Хранилище над драйвером ──────────────────────────────────────────

-- Договор отказа тот же: чтение при отказе клиента — промах и запись
-- в журнале, запись — бросок самого отказа.
g.test_store_keeps_the_failure_contract = function()
    local failure = setmetatable({ kind = 'unreachable' }, {
        __tostring = function()
            return 'redis 127.0.0.1:1: соединение не открылось'
        end,
    })
    local client = helper.fake_redis({
        { nil, failure },
        { nil, failure },
        { nil, failure },
        { nil, failure },
    })
    local store = cache.new({
        driver = 'redis',
        redis = client,
        namespace = 'app:',
        prefix = 'demo:',
        now = helper.clock().realtime,
    })

    t.assert_equals(store:get('a', 'нет'), 'нет')
    t.assert_equals(store:pull('token', 'нет'), 'нет', 'забрать не вышло — промах')

    local ok, err = pcall(store.put, store, 'a', 1)

    t.assert_equals(ok, false)
    t.assert_is(err, failure, 'запись бросает сам отказ')

    ok, err = pcall(store.add, store, 'lock', true, 10)

    t.assert_equals(ok, false)
    t.assert_is(err, failure)

    local records = helper.journal.records()

    t.assert_equals(#records, 1, 'повтор подряд подавлен')
    t.assert_equals(records[1].record.fields, {
        driver = 'redis',
        err = 'redis 127.0.0.1:1: соединение не открылось',
    })
    t.assert_equals(client.calls[1].args, { 'GET', 'app:demo:a' })
    t.assert_equals(client.calls[2].args, { 'GETDEL', 'app:demo:token' })
end

-- ── Теги и замок ─────────────────────────────────────────────────────

-- Запись с тегами — один сценарий: ключ записи и множества тегов в KEYS,
-- счёт ключа, затем остаток SET. Повтор после отправки безвреден.
g.test_tagged_write_is_one_script = function()
    local d, client = driver({ { 'OK' }, { 'OK' } })

    d.write('a', { id = 7 }, cache.store.FOREVER, { 'docs', 'p:all' })
    d.write('n', 5, 1000.0004, { 'docs' })

    t.assert_equals(client.calls, {
        {
            args = {
                'EVAL',
                cache.redis.TAGGED,
                3,
                'cache:a',
                'cache:\0tag:docs',
                'cache:\0tag:p:all',
                '+inf',
                msgpack.encode({ id = 7 }),
            },
            opts = REPEATABLE,
        },
        {
            args = {
                'EVAL',
                cache.redis.TAGGED,
                2,
                'cache:n',
                'cache:\0tag:docs',
                1000001,
                double(5),
                'PXAT',
                1000001,
            },
            opts = REPEATABLE,
        },
    })
    t.assert_equals(cache.redis.TAG, '\0tag:')
end

-- add с тегами — тот же сценарий с NX и без повтора: ответ сценария пуст,
-- если ключ занят.
g.test_tagged_add_is_the_script_with_nx = function()
    local d, client = driver({ { 'OK' }, {} })

    t.assert_equals(d.add('lock', true, 1000.5, { 'docs' }), true)
    t.assert_equals(d.add('lock', true, 0, { 'docs' }), false)

    local lock = msgpack.encode(true)

    t.assert_equals(client.calls, {
        {
            args = {
                'EVAL',
                cache.redis.TAGGED,
                2,
                'cache:lock',
                'cache:\0tag:docs',
                1000500,
                lock,
                'PXAT',
                1000500,
                'NX',
            },
        },
        { args = { 'EVAL', cache.redis.TAGGED, 2, 'cache:lock', 'cache:\0tag:docs', '+inf', lock, 'NX' } },
    })
end

-- Забывание тега — кусок сценария: сколько взято и сколько стёрто.
g.test_erase_tagged_is_a_chunk_of_the_script = function()
    local d, client = driver({ { { 3, 2 } } }, { namespace = 'app:' })

    t.assert_equals({ d.erase_tagged('p:docs', 1000) }, { 3, 2 })
    t.assert_equals(client.calls, {
        { args = { 'EVAL', cache.redis.FORGET_TAG, 1, 'app:\0tag:p:docs', 1000 }, opts = REPEATABLE },
    })
end

-- Снятие замка — сценарий «стереть, если метка своя»: 1 — снят, пусто —
-- не наш.
g.test_erase_if_is_one_script = function()
    local d, client = driver({ { 1 }, {} })

    t.assert_equals(d.erase_if('p:\0lock:r', 'метка'), true)
    t.assert_equals(d.erase_if('p:\0lock:r', 'метка'), false)
    t.assert_equals(client.calls[1], {
        args = { 'EVAL', cache.redis.RELEASE, 1, 'cache:p:\0lock:r', msgpack.encode('метка') },
        opts = REPEATABLE,
    })
end

-- Хранилище над Redis берёт замок одним SET NX и снимает сценарием;
-- тег приходит к серверу с приставкой хранилища.
g.test_store_locks_and_tags_through_the_server = function()
    local client = helper.fake_redis({ { 'OK' }, { 1 }, { 'OK' }, { { 1, 1 } } })
    local clock = helper.clock()
    local store = cache.new({ driver = 'redis', redis = client, prefix = 'p:', now = clock.realtime })

    cache.store._set_source({
        token = function()
            return 'метка'
        end,
        yield = function() end,
    })

    local lock = store:lock('report', 10)

    t.assert_equals(lock:release(), true)

    store:tags('docs'):put('a', 1)

    t.assert_equals(store:tags('docs'):flush(), 1)

    local token = msgpack.encode('метка')
    local moment = (clock.realtime() + 10) * 1000

    t.assert_equals(client.calls[1].args, { 'SET', 'cache:p:\0lock:report', token, 'NX', 'PXAT', moment })
    t.assert_equals(client.calls[2].args[4], 'cache:p:\0lock:report')
    t.assert_equals(client.calls[3].args[5], 'cache:\0tag:p:docs')
    t.assert_equals(client.calls[4].args[4], 'cache:\0tag:p:docs')
end
