--- Тесты кэша: срок, приставка, действия хранилища, оба драйвера.

local t = require('luatest')

local helper = dofile('test/helper.lua')

local g = t.group('tnt.cache')

---@type any
local cache

---@type table
local clock

g.before_each(function()
    cache = helper.fresh()
    clock = helper.clock()
end)

g.after_each(helper.unload)

--- Хранилище в памяти с проверочными часами.
---@param opts table|nil
---@return any
local function store(opts)
    return helper.store(cache, clock, opts)
end

--- Хранилище над двойником спейса: транзакция — просто вызов, инстанса нет.
---
--- Ключи у двойника упорядочены, как у TREE, поэтому куски и партии
--- обхода здесь предсказуемы — в отличие от снимка таблицы в памяти.
---@param opts table|nil
---@return any store
---@return { scans: integer[], erased: string[][] } calls
local function fake_store(opts)
    cache.space._set_source({
        atomic = function(fn, ...)
            return fn(...)
        end,
    })

    local driver, calls = helper.counting_driver(cache.space.new(helper.fake_space()))

    opts = opts or {}
    opts.driver = driver
    opts.now = clock.realtime

    return cache.new(opts), calls
end

--- Кладёт n ключей k1..kn со сроком в index секунд каждый.
---@param s any
---@param n integer
local function fill(s, n)
    for index = 1, n do
        s:put('k' .. index, index, index)
    end
end

-- ── Значения и срок ──────────────────────────────────────────────────

g.test_put_get_and_forget = function()
    local s = store()

    t.assert_equals(s:get('a', 'нет'), 'нет')
    t.assert_equals(s:has('a'), false)

    s:put('a', { id = 7 })

    t.assert_equals(s:get('a'), { id = 7 })
    t.assert_equals(s:has('a'), true)

    s:forget('a')

    t.assert_equals(s:get('a'), nil)
    t.assert_equals(s:status(), { name = 'memory', driver = 'memory', prefix = '', ttl = nil })
end

g.test_value_expires_after_its_ttl_and_is_erased_on_read = function()
    local s = store()

    s:put('a', 1, 10)
    clock.advance(9)

    t.assert_equals(s:get('a'), 1)

    clock.advance(1)

    t.assert_equals(s:get('a'), nil, 'срок вышел ровно в секунду')
    t.assert_equals(s.driver.read('a'), nil, 'просроченное стёрто по дороге')

    s:put('b', 1, 10)
    clock.advance(25)

    t.assert_equals(s:get('b'), nil, 'и много позже срока — тоже')

    s:put('c', 1, 1)
    clock.advance(1)

    t.assert_equals(s:get('c'), nil, 'срок в одну секунду — тоже срок')
end

g.test_default_ttl_applies_and_explicit_forever_is_nil = function()
    local s = store({ ttl = 5 })

    s:put('a', 1)
    clock.advance(5)

    t.assert_equals(s:get('a'), nil)

    local forever = store()

    forever:put('b', 1)
    clock.advance(1e9)

    t.assert_equals(forever:get('b'), 1)
end

g.test_prefix_separates_neighbours_on_one_driver = function()
    local driver = cache.memory.new()
    local left = cache.new({ driver = driver, prefix = 'l:', now = clock.realtime })
    local right = cache.new({ driver = driver, prefix = 'r:', now = clock.realtime })

    left:put('a', 'левое')
    right:put('a', 'правое')

    t.assert_equals(left:get('a'), 'левое')
    t.assert_equals(right:get('a'), 'правое')

    left:flush()

    t.assert_equals(right:get('a'), nil, 'flush чистит драйвер целиком')
end

-- ── Действия ─────────────────────────────────────────────────────────

g.test_add_only_when_absent = function()
    local s = store()

    t.assert_equals(s:add('a', 1, 10), true)
    t.assert_equals(s:add('a', 2, 10), false)
    t.assert_equals(s:get('a'), 1)

    clock.advance(10)

    t.assert_equals(s:add('a', 2), true, 'просроченное — отсутствующее')
end

g.test_remember_computes_once = function()
    local s = store()
    local calls = 0

    local compute = function()
        calls = calls + 1

        return 'вычислено'
    end

    t.assert_equals(s:remember('a', 10, compute), 'вычислено')
    t.assert_equals(s:remember('a', 10, compute), 'вычислено')
    t.assert_equals(calls, 1)

    clock.advance(10)
    s:remember('a', 10, compute)

    t.assert_equals(calls, 2)
end

g.test_pull_takes_and_forgets = function()
    local s = store()

    s:put('a', 1)

    t.assert_equals(s:pull('a'), 1)
    t.assert_equals(s:get('a'), nil)
    t.assert_equals(s:pull('a', 'нет'), 'нет')
end

-- Логика — не число: счётчик поверх неё не начинается с нуля.
g.test_increment_does_not_count_over_a_boolean = function()
    local s = store()

    s:put('flag', false)

    t.assert_error_msg_equals(
        'кэш: flag — не число, прибавлять нечего',
        s.increment,
        s,
        'flag'
    )
    t.assert_equals(s:get('flag'), false, 'значение цело')
end

-- Драйвер, умеющий действие одним шагом, делает его сам: хранилище
-- отдаёт ему ключ с приставкой и срок, а само не читает и не пишет.
g.test_one_step_actions_are_left_to_the_driver = function()
    local inner = cache.memory.new()
    local calls = {}

    local driver = setmetatable({
        add = function(key, value, expires_at)
            table.insert(calls, { 'add', key, value, expires_at })

            return false
        end,
        take = function(key)
            table.insert(calls, { 'take', key })

            return 'знак', 0
        end,
        increment = function(key, by, expires_at)
            table.insert(calls, { 'increment', key, by, expires_at })

            return by * 10
        end,
        read = function()
            error('читать было нечего', 0)
        end,
        write = function()
            error('писать было нечего', 0)
        end,
        erase = function()
            error('стирать было нечего', 0)
        end,
    }, { __index = inner })

    local s = cache.new({ driver = driver, prefix = 'p:', ttl = 30, now = clock.realtime })
    local now = clock.realtime()

    t.assert_equals(s:add('lock', 'я', 10), false)
    t.assert_equals(s:add('lock', 'я'), false)
    t.assert_equals(s:pull('token'), 'знак')
    t.assert_equals(s:increment('hits'), 10)
    t.assert_equals(s:increment('hits', 3), 30)
    t.assert_equals(s:decrement('hits', 2), -20)
    t.assert_equals(s:decrement('hits'), -10)
    t.assert_equals(calls, {
        { 'add', 'p:lock', 'я', now + 10 },
        { 'add', 'p:lock', 'я', now + 30 },
        { 'take', 'p:token' },
        { 'increment', 'p:hits', 1, now + 30 },
        { 'increment', 'p:hits', 3, now + 30 },
        { 'increment', 'p:hits', -2, now + 30 },
        { 'increment', 'p:hits', -1, now + 30 },
    })

    t.assert_error_msg_equals(
        'кэш: значение не может быть пустым, для удаления есть forget',
        s.add,
        s,
        'lock',
        nil
    )

    driver.increment = function()
        return nil
    end

    t.assert_error_msg_equals(
        'кэш: hits — не число, прибавлять нечего',
        s.increment,
        s,
        'hits'
    )

    driver.take = function()
        return nil
    end

    t.assert_equals(s:pull('token', 'нет'), 'нет')
end

-- Драйвер по сети внутри транзакции box — ошибка программиста: бросок
-- до обращения к драйверу, а не промах в журнал. Драйверу в памяти
-- транзакция не мешает.
g.test_remote_driver_is_refused_inside_a_transaction = function()
    cache.store._set_source({
        in_transaction = function()
            return true
        end,
    })

    local client = helper.fake_redis({})
    local remote = cache.new({ driver = 'redis', redis = client, now = clock.realtime })
    local refused = 'кэш: драйвер redis ходит по сети — внутри транзакции box его не зовут: '
        .. 'уступка порвёт транзакцию, а записанное в него при откате останется'

    local actions = {
        { 'get', 'a' },
        { 'has', 'a' },
        { 'put', 'a', 1 },
        { 'add', 'a', 1 },
        { 'remember', 'a', 10, tostring },
        { 'pull', 'a' },
        { 'forget', 'a' },
        { 'increment', 'a' },
        { 'decrement', 'a' },
        { 'flush' },
    }

    for _, action in ipairs(actions) do
        t.assert_error_msg_equals(refused, remote[action[1]], remote, unpack(action, 2))
    end

    t.assert_equals(client.calls, {}, 'к серверу не ходили')
    t.assert_equals(helper.journal.records(), {}, 'ошибка программиста — не промах')

    local local_store = store()

    local_store:put('a', 1)
    local_store:flush()

    t.assert_equals(local_store:get('a'), nil)
end

g.test_increment_and_decrement_keep_the_ttl = function()
    local s = store({ ttl = 100 })

    t.assert_equals(s:increment('hits'), 1)
    t.assert_equals(s:increment('hits', 5), 6)
    t.assert_equals(s:decrement('hits'), 5)
    t.assert_equals(s:decrement('hits', 2), 3)

    s:put('short', 1, 10)
    clock.advance(5)
    s:increment('short')
    clock.advance(5)

    t.assert_equals(s:get('short'), nil, 'срок записи не продлевается прибавлением')

    s:put('word', 'слово')

    t.assert_error_msg_equals(
        'кэш: word — не число, прибавлять нечего',
        s.increment,
        s,
        'word'
    )
end

g.test_sweep_erases_only_the_expired = function()
    local s = store()

    s:put('a', 1, 10)
    s:put('b', 2, 20)
    s:put('c', 3)
    clock.advance(10)

    t.assert_equals(
        s:sweep(),
        { swept = 1, scanned = 3 },
        'срок вышел ровно в секунду — стёрто'
    )
    t.assert_equals(s.driver.read('b'), 2)
    t.assert_equals(s.driver.read('c'), 3, 'навсегда — не срок')
    t.assert_equals(s:sweep(), { swept = 0, scanned = 2 })
end

-- ── Обход кусками ────────────────────────────────────────────────────

-- Перед каждым куском — уступка, у драйвера просят не больше chunk
-- ключей, просроченное куска стирается одной партией, а кусок без
-- просроченного партии не стирает.
g.test_sweep_walks_in_chunks_with_a_yield_before_each = function()
    local s, calls = fake_store()
    local yields = 0

    cache.store._set_source({
        yield = function()
            yields = yields + 1
        end,
    })

    fill(s, 5)
    clock.advance(3)

    t.assert_equals(s:sweep({ chunk = 2 }), { swept = 3, scanned = 5 })
    t.assert_equals(yields, 3, 'пять ключей по два — три куска')
    t.assert_equals(calls.scans, { 2, 2, 2 })
    t.assert_equals(calls.erased, { { 'k1', 'k2' }, { 'k3' } })
    t.assert_equals(s:get('k4'), 4)
end

-- Умолчание куска — тысяча: 1001 ключ — ровно два куска.
g.test_sweep_takes_a_thousand_keys_per_chunk_by_default = function()
    local s, calls = fake_store()
    local yields = 0

    cache.store._set_source({
        yield = function()
            yields = yields + 1
        end,
    })

    fill(s, 1001)

    t.assert_equals(s:sweep(), { swept = 0, scanned = 1001 })
    t.assert_equals(calls.scans, { 1000, 1000 })
    t.assert_equals(yields, 2)
end

-- Бюджет ограничивает просмотренное за вызов, курсор продолжает с того же
-- места, и просмотренное заново не просматривается.
g.test_sweep_budget_hands_out_a_cursor = function()
    local s, calls = fake_store()

    fill(s, 7)
    clock.advance(7)

    local first = s:sweep({ budget = 3, chunk = 2 })

    t.assert_equals(first, { swept = 3, scanned = 3, cursor = 'k3' })
    t.assert_equals(
        calls.scans,
        { 2, 1 },
        'последний кусок бюджета — остаток, а не chunk'
    )

    local second = s:sweep({ budget = 3, chunk = 2, cursor = first.cursor })

    t.assert_equals(second, { swept = 3, scanned = 3, cursor = 'k6' })

    local third = s:sweep({ budget = 3, chunk = 2, cursor = second.cursor })

    t.assert_equals(third, { swept = 1, scanned = 1 })
    t.assert_equals(s:sweep(), { swept = 0, scanned = 0 })
end

-- Обход, кончившийся ровно на бюджете, вправе отдать курсор: драйвер
-- не знает, остались ли ключи. Следующий вызов — пустой отчёт без курсора.
g.test_sweep_ending_on_the_budget_returns_an_empty_report_next = function()
    local s = fake_store()

    fill(s, 4)

    local first = s:sweep({ budget = 4, chunk = 2 })

    t.assert_equals(first, { swept = 0, scanned = 4, cursor = 'k4' })
    t.assert_equals(s:sweep({ budget = 4, chunk = 2, cursor = first.cursor }), { swept = 0, scanned = 0 })
end

-- Часы читаются на каждый кусок: запись, чей срок вышел за уступкой,
-- стирается этим же обходом.
g.test_sweep_reads_the_clock_per_chunk = function()
    local s = fake_store()

    cache.store._set_source({
        yield = function()
            clock.advance(10)
        end,
    })

    s:put('k1', 1, 15)
    s:put('k2', 2, 15)

    t.assert_equals(s:sweep({ chunk = 1 }), { swept = 1, scanned = 2 })
    t.assert_equals(s.driver.read('k1'), 1, 'на первом куске срок ещё не вышел')
    t.assert_equals(s.driver.read('k2'), nil, 'на втором — вышел')
end

g.test_sweep_refuses_bad_options_and_transactions = function()
    local s = store()

    t.assert_error_msg_equals(
        'кэш: настройки sweep должны быть таблицей',
        s.sweep,
        s,
        'всё'
    )

    for _, name in ipairs({ 'chunk', 'budget' }) do
        for _, bad in ipairs({ 0, 1.5, -1, 'много' }) do
            t.assert_error_msg_equals(
                ('кэш: %s должен быть целым числом больше нуля'):format(name),
                s.sweep,
                s,
                { [name] = bad }
            )
        end
    end

    s:put('a', 1)

    t.assert_covers(
        s:sweep({ chunk = 1, budget = 1 }),
        { swept = 0, scanned = 1 },
        'единица — уже число'
    )

    cache.store._set_source({
        in_transaction = function()
            return true
        end,
    })

    t.assert_error_msg_equals(
        'кэш: sweep уступает управление и сам фиксирует партии — внутри транзакции его не зовут',
        s.sweep,
        s
    )
end

-- Отказ box посреди обхода — пара с числом стёртого до отказа: реплика
-- или снесённый спейс — не поломка чистки. Поломка драйвера — бросок
-- как есть, а отмена файбера в отказ не прячется.
g.test_sweep_turns_box_failures_into_a_refusal_and_rethrows_the_rest = function()
    local s, calls = fake_store()
    local driver = s.driver
    local inner = driver.erase_many

    fill(s, 3)
    clock.advance(3)

    -- Отказ box без инстанса: `box.error.new` есть и до `box.cfg`,
    -- а в аннотациях ядра он необязателен.
    ---@type any
    local errors = box.error

    driver.erase_many = function(keys)
        if #calls.erased == 1 then
            error(errors.new({ code = 77, reason = 'спейс снесли' }))
        end

        return inner(keys)
    end

    t.assert_equals({ s:sweep({ chunk = 2 }) }, {
        nil,
        'кэш: чистка не дошла до конца — стёрто 2, дальше box отказал: спейс снесли',
    })
    t.assert_equals(s.driver.read('k1'), nil, 'стёртое до отказа остаётся стёртым')
    t.assert_equals(s.driver.read('k3'), 3)

    driver.scan = function()
        error('драйвер сломан', 0)
    end

    t.assert_error_msg_equals('драйвер сломан', s.sweep, s)

    local fiber = require('fiber')

    cache.store._set_source({
        yield = function()
            fiber.self():cancel()
            fiber.yield()
        end,
    })

    driver.scan = inner

    local worker = fiber.new(function()
        s:sweep()
    end)

    worker:set_joinable(true)

    local joined, failure = worker:join()

    t.assert_equals({ joined, tostring(failure) }, { false, 'fiber is cancelled' })
end

-- ── Драйвер в памяти ─────────────────────────────────────────────────

-- Обход в памяти идёт по снимку ключей: стёртый после снимка ключ
-- пропускается, дописанный — ждёт следующего обхода, а очищенная таблица
-- обрывает обход пустым куском.
g.test_memory_driver_scans_a_snapshot_of_keys = function()
    local driver = cache.memory.new()

    for index = 1, 5 do
        driver.write('k' .. index, index, index)
    end

    local seen = {}
    local batch, cursor = driver.scan(nil, 2)

    t.assert_equals(#batch, 2)
    t.assert_not_equals(cursor, nil)

    for _, entry in ipairs(batch) do
        seen[entry.key] = entry.expires_at
    end

    local pending

    for index = 1, 5 do
        if seen['k' .. index] == nil then
            pending = 'k' .. index
        end
    end

    driver.erase(pending)
    driver.write('new', 0, 0)

    for _ = 1, 10 do
        batch, cursor = driver.scan(cursor, 2)

        for _, entry in ipairs(batch) do
            seen[entry.key] = entry.expires_at
        end

        if cursor == nil then
            break
        end
    end

    t.assert_equals(
        cursor,
        nil,
        'снимок из пяти ключей обходится не дольше трёх кусков'
    )

    local expected = {}

    for index = 1, 5 do
        if 'k' .. index ~= pending then
            expected['k' .. index] = index
        end
    end

    t.assert_equals(seen, expected)

    local fresh = driver.scan(nil, 10)

    t.assert_equals(#fresh, 5, 'новый обход видит дописанный ключ')

    local head, rest = driver.scan(nil, 3)

    driver.clear()

    t.assert_equals(#head, 3)
    t.assert_equals(
        { driver.scan(rest, 3) },
        { {} },
        'после очистки остаток пуст, курсора нет'
    )
end

g.test_memory_driver_erases_a_batch = function()
    local driver = cache.memory.new()

    driver.write('a', 1, 0)
    driver.write('b', 2, 0)
    driver.write('c', 3, 0)
    driver.erase_many({ 'a', 'c' })

    t.assert_equals(driver.read('a'), nil)
    t.assert_equals(driver.read('b'), 2)
    t.assert_equals(driver.read('c'), nil)
end

-- ── Отказы ───────────────────────────────────────────────────────────

g.test_refusals_name_the_reason = function()
    local s = store()

    t.assert_error_msg_equals('кэш: ключ должен быть непустой строкой', s.get, s, 7)
    t.assert_error_msg_equals('кэш: ключ должен быть непустой строкой', s.put, s, '', 1)
    t.assert_error_msg_equals(
        'кэш: значение не может быть пустым, для удаления есть forget',
        s.put,
        s,
        'a',
        nil
    )
    t.assert_error_msg_equals(
        'кэш: срок должен быть числом секунд больше нуля',
        s.put,
        s,
        'a',
        1,
        0
    )
    t.assert_error_msg_equals(
        'кэш: срок должен быть числом секунд больше нуля',
        s.put,
        s,
        'a',
        1,
        'скоро'
    )
    t.assert_error_msg_equals(
        'кэш: срок должен быть числом секунд больше нуля',
        s.put,
        s,
        'a',
        1,
        -1
    )
    t.assert_error_msg_equals('кэш: prefix должна быть строкой', cache.new, { prefix = 7 })
    t.assert_error_msg_equals(
        'кэш: ttl должен быть числом секунд больше нуля',
        cache.new,
        { ttl = 0 }
    )
    t.assert_equals(cache.new({ ttl = 1 }):status().ttl, 1, 'секунда — уже срок')
    t.assert_error_msg_equals(
        'кэш: ttl должен быть числом секунд больше нуля',
        cache.new,
        { ttl = -1 }
    )
    t.assert_error_msg_equals(
        'кэш: драйвера memcached нет, есть memory, space и redis',
        cache.new,
        { driver = 'memcached' }
    )
    t.assert_error_msg_equals(
        'кэш: шаг прибавки должен быть числом',
        s.increment,
        s,
        'hits',
        '2'
    )
    t.assert_error_msg_equals(
        'кэш: шаг прибавки должен быть числом',
        s.decrement,
        s,
        'hits',
        true
    )
    t.assert_equals(s:get('hits'), nil, 'негодный шаг ничего не записал')
    t.assert_error_msg_equals('кэш: драйверу space нужен спейс', cache.space.new, 'app_cache')
end

-- ── Отказ драйвера ───────────────────────────────────────────────────

--- Ломает названное действие драйвера: с этого мига оно отказывает.
---@param s any Хранилище
---@param action string Имя действия драйвера: read, write, erase, clear
---@param reason string Чем оно отказывает
local function breaks(s, action, reason)
    s.driver[action] = function()
        error(reason, 0)
    end
end

-- Чтение при отказе драйвера промахивается, а не бросает: вызывающему
-- промах безопасен. Молчать о нём нельзя — о промахе есть запись в
-- журнале, и одна на вид отказа: повтор подряд подавлен и сосчитан.
g.test_driver_failure_on_read_is_a_miss_with_one_record = function()
    local s = store()

    s:put('a', 1)
    breaks(s, 'read', 'связи с кэшем нет')

    t.assert_equals(s:get('a', 'нет'), 'нет')
    t.assert_equals(s:has('a'), false)

    local records = helper.journal.records()

    t.assert_equals(#records, 1, 'вторая такая же беда журнал не топит')
    t.assert_equals(records[1].level, 'warn')
    t.assert_equals(records[1].module, 'tnt.cache')
    t.assert_equals(
        records[1].record.message,
        'кэш: драйвер memory отказал на чтении — промах'
    )
    t.assert_equals(records[1].record.fields, { driver = 'memory', err = 'связи с кэшем нет' })

    breaks(s, 'read', 'спейса нет')

    t.assert_equals(s:get('a', 'нет'), 'нет')

    local second = helper.journal.records()[2]

    t.assert_equals(second.record.fields.err, 'спейса нет')
    t.assert_equals(second.record.suppressed, 1, 'подавленный повтор сосчитан')
end

-- Промах достаётся и составным действиям: `remember` считает заново,
-- `pull` отдаёт умолчание и не стирает — непрочитанное значение
-- достанется следующему вызову, а не пропадёт молча.
g.test_read_failure_recomputes_and_takes_nothing_away = function()
    local s = store()
    local erased = 0

    s:put('a', 1)
    s.driver.erase = function()
        erased = erased + 1
    end
    breaks(s, 'read', 'связи с кэшем нет')

    t.assert_equals(s:pull('a', 'нет'), 'нет')
    t.assert_equals(erased, 0, 'нечего было забрать — нечего и стирать')
    t.assert_equals(
        s:remember('a', 10, function()
            return 'считано'
        end),
        'считано'
    )
end

-- Запись при отказе хранилища бросает: потерянную запись не заметит
-- никто, и промахом она не прикрывается — записи в журнале о ней нет.
g.test_writes_throw_when_the_driver_refuses = function()
    local s = store()

    breaks(s, 'write', 'на реплике не пишут')

    t.assert_error_msg_equals('на реплике не пишут', s.put, s, 'a', 1)
    t.assert_error_msg_equals('на реплике не пишут', s.increment, s, 'hits')

    breaks(s, 'erase', 'на реплике не стирают')

    t.assert_error_msg_equals('на реплике не стирают', s.forget, s, 'a')

    breaks(s, 'clear', 'спейса нет')

    t.assert_error_msg_equals('спейса нет', s.flush, s)
    t.assert_equals(helper.journal.records(), {}, 'бросок — не промах')
end

-- Чтение, от которого зависит запись, тоже бросает: промах у `add`
-- означал бы «пиши поверх чужого», у счётчика — «считай с нуля».
g.test_reads_that_decide_a_write_throw = function()
    local s = store()

    breaks(s, 'read', 'связи с кэшем нет')

    t.assert_error_msg_equals('связи с кэшем нет', s.add, s, 'lock', true, 10)
    t.assert_error_msg_equals('связи с кэшем нет', s.increment, s, 'hits')
    t.assert_error_msg_equals('связи с кэшем нет', s.decrement, s, 'hits')
    t.assert_equals(helper.journal.records(), {}, 'бросок — не промах')
end

-- Отмена файбера в промах не прячется: это приказ остановиться, а не
-- отказ драйвера, и отменённый файбер обязан умереть.
g.test_cancelled_fiber_is_not_a_miss = function()
    local fiber = require('fiber')
    local s = store()

    s.driver.read = function()
        fiber.self():cancel()
        fiber.yield()
    end

    local worker = fiber.new(function()
        s:get('a')
    end)

    worker:set_joinable(true)

    local joined, failure = worker:join()

    t.assert_equals({ joined, tostring(failure) }, { false, 'fiber is cancelled' })
    t.assert_equals(helper.journal.records(), {}, 'отмена — не отказ драйвера')
end

-- На узле только для чтения просроченное читается как отсутствующее и
-- остаётся лежать: стирать его там нечем, а чтение не смеет падать
-- оттого, что у записи вышел срок. Стереть остаётся ведущему и sweep.
g.test_read_only_node_keeps_the_expired_instead_of_erasing_it = function()
    local s = store()

    s:put('a', 1, 10)
    clock.advance(10)

    cache.store._set_source({
        read_only = function()
            return true
        end,
    })

    t.assert_equals(s:get('a', 'нет'), 'нет')
    t.assert_equals(s:has('a'), false)
    t.assert_equals(s.driver.read('a'), 1, 'запись цела: стирать её не пробовали')
    t.assert_equals(helper.journal.records(), {}, 'промах по сроку — не отказ')
    t.assert_equals(s:sweep(), { swept = 1, scanned = 1 }, 'чистке запрет не писан')
end

-- ── Драйвер на спейсе ────────────────────────────────────────────────

g.test_space_driver_keeps_rows_as_key_value_expiry = function()
    local space = helper.fake_space()
    local s = cache.new({ driver = 'space', space = space, now = clock.realtime })
    local transactions = 0

    cache.space._set_source({
        atomic = function(fn, ...)
            transactions = transactions + 1

            return fn(...)
        end,
    })

    t.assert_equals(s:get('a', 'нет'), 'нет', 'пустой спейс — пустой кэш')

    s:put('a', { id = 7 }, 10)
    s:put('b', 'навсегда')

    t.assert_equals(space:get('a'), { 'a', { id = 7 }, 1700000010 })
    t.assert_equals(space:get('b').expires_at, 0)
    t.assert_equals(s:get('a'), { id = 7 })
    t.assert_equals(s:status().driver, 'space')

    clock.advance(10)

    t.assert_equals(s:get('a'), nil)
    t.assert_equals(space:get('a'), nil, 'просроченное удалено из спейса')
    t.assert_equals(s:sweep(), { swept = 0, scanned = 1 })
    t.assert_equals(transactions, 0, 'нечего стирать — нет и транзакции')

    s:put('c', 1, 1)
    s:put('d', 1, 1)
    clock.advance(1)

    t.assert_equals(s:sweep(), { swept = 2, scanned = 3 })
    t.assert_equals(transactions, 1, 'партия стирается одной транзакцией')
    t.assert_equals(space:get('c'), nil)

    s:flush()

    t.assert_equals(space:get('b'), nil)
end

-- Партии по GT с limit: полный кусок отдаёт курсор — последний ключ, —
-- неполный кончает обход; первичный индекс обязан быть TREE.
g.test_space_driver_scans_by_gt_with_limit = function()
    local space = helper.fake_space()
    local driver = cache.space.new(space)

    for index = 1, 4 do
        driver.write('k' .. index, index, index)
    end

    local batch, cursor = driver.scan(nil, 2)

    t.assert_equals({ batch[1].key, batch[2].key, cursor }, { 'k1', 'k2', 'k2' })

    batch, cursor = driver.scan(cursor, 2)

    t.assert_equals(
        { batch[1].key, batch[2].key, cursor },
        { 'k3', 'k4', 'k4' },
        'полный кусок — курсор есть'
    )
    t.assert_equals(
        { driver.scan(cursor, 2) },
        { {} },
        'за последним ключом пусто и курсора нет'
    )

    driver.erase('k4')

    batch, cursor = driver.scan('k2', 2)

    t.assert_equals({ batch[1].key, cursor }, { 'k3', nil }, 'неполный кусок кончает обход')
    t.assert_error_msg_equals(
        'кэш: драйверу space нужен спейс с первичным индексом TREE — по HASH обход от стёртого ключа обрывается',
        cache.space.new,
        { index = { [0] = { type = 'HASH' } } }
    )
end

-- Спейс по имени ищется первым обращением, а не при сборке: кэш собирают
-- при применении роли, а спейс заводит шаг миграции после него. До шага
-- чтение — промах, запись — бросок, чистка — пара, как у снесённого
-- спейса; после шага то же хранилище работает без пересборки.
g.test_space_driver_by_name_finds_the_space_on_first_use = function()
    local spaces, asked = helper.named_spaces(cache)
    local s = cache.new({ driver = 'space', space = 'other', now = clock.realtime })
    local missing = 'кэш: спейса other нет — нужен шаг миграции tnt.cache.space.migrate'
    local stopped = 'кэш: чистка не дошла до конца — стёрто 0, дальше box отказал: '
        .. missing

    t.assert_equals(asked(), {}, 'сборка спейса не ищет')
    t.assert_equals(s:status().driver, 'space')
    t.assert_equals(s:get('a', 'нет'), 'нет', 'до шага чтение — промах')
    t.assert_equals(helper.journal.records()[1].record.fields, { driver = 'space', err = missing })
    t.assert_error_msg_equals(missing, s.put, s, 'a', 1)
    t.assert_error_msg_equals(missing, s.forget, s, 'a')
    t.assert_equals(
        { s:sweep() },
        { nil, stopped },
        'спейса нет, как снесённого: чистка отдаёт пару'
    )
    t.assert_equals(asked(), { 'other', 'other', 'other', 'other' }, 'не нашли — ищут снова')

    -- Шаг прошёл: спейс есть, спейса тегов нет.
    spaces.other = helper.fake_space()

    s:put('a', 1, 5)

    t.assert_equals(spaces.other:get('a'), { 'a', 1, 1700000005 })
    t.assert_equals(s:get('a'), 1)
    t.assert_equals(
        asked(),
        { 'other', 'other_tags', 'other', 'other_tags' },
        'пока спейса тегов нет, его ищут на каждом обращении'
    )
    t.assert_equals(s:add('b', 2), true, 'вставка без тегов идёт и без спейса тегов')
    t.assert_equals(s:add('b', 3), false)

    clock.advance(5)

    t.assert_equals(s:sweep(), { swept = 1, scanned = 2 })

    s:flush()

    t.assert_equals(spaces.other:get('b'), nil)
end

g.test_migration_creates_the_space_once = function()
    local created = {}
    local spaces = {}
    local fake_box = {
        space = spaces,
        schema = {
            space = {
                create = function(name, opts)
                    table.insert(created, { name = name, format = opts.format })

                    local space = {
                        indexes = {},
                    }

                    space.create_index = function(_, index_name, index_opts)
                        space.indexes[index_name] = index_opts.parts
                    end

                    spaces[name] = space

                    return space
                end,
            },
        },
    }

    cache.space.migrate(fake_box)
    cache.space.migrate(fake_box)
    cache.space.migrate(fake_box, 'other')

    t.assert_equals(#created, 4)
    t.assert_equals(created[1].name, 'app_cache')
    t.assert_equals(created[2].name, 'app_cache_tags')
    t.assert_equals(created[3].name, 'other')
    t.assert_equals(created[4].name, 'other_tags')
    t.assert_equals(created[1].format, {
        { name = 'key', type = 'string' },
        { name = 'value', type = 'any' },
        { name = 'expires_at', type = 'number' },
        { name = 'tags', type = 'array', is_nullable = true },
    })
    t.assert_equals(created[2].format, {
        { name = 'tag', type = 'string' },
        { name = 'key', type = 'string' },
    })
    t.assert_equals(spaces.app_cache.indexes.primary, { 'key' })
    ---@type any
    local tags_space = spaces.app_cache_tags

    t.assert_equals(tags_space.indexes.primary, { 'tag', 'key' })

    -- Спейс, заведённый до тегов, доводится тем же шагом: дописывается
    -- спейс тегов, а сам спейс не трогается.
    spaces.app_cache_tags = nil
    cache.space.migrate(fake_box)

    t.assert_equals(#created, 5)
    t.assert_equals(created[5].name, 'app_cache_tags')
    t.assert_equals(cache.space.TAGS, '_tags')
end
