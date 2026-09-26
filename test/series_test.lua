--- Проверки рядов метрик кэша: итог обращения за значением, длительность
--- чтения драйвером и имя хранилища в метке.
---
--- Ряды читаются из реестра встроенного `metrics` так же, как их видит
--- сборщик. Исходники рядов грузятся заново каждой проверкой вместе
--- с пакетом, и счёт у каждой проверки свой, с нуля.

local t = require('luatest')

local helper = dofile('test/helper.lua')

local g = t.group('tnt.cache.series')

---@type any
local cache

---@type any
local series

---@type table
local clock

g.before_each(function()
    cache = helper.fresh()
    series = helper.module('tnt.cache.series')
    clock = helper.clock()
end)

g.after_each(function()
    cache.store._set_source(nil)
    helper.unload()
end)

--- Хранилище в памяти с проверочными часами.
---@param opts table|nil
---@return any
local function store(opts)
    return helper.store(cache, clock, opts)
end

--- Итоги обращений хранилища по словам: пусто — таких не было.
---@param name string Имя хранилища
---@return table<string, number|nil>
local function outcomes(name)
    local found = {}

    for _, outcome in ipairs({ 'hit', 'miss', 'failed' }) do
        found[outcome] = helper.value('cache_requests_total', { store = name, outcome = outcome })
    end

    return found
end

--- Сколько обращений хранилища измерено.
---@param name string
---@return number|nil
local function measured(name)
    return helper.value('cache_request_duration_seconds_count', { store = name })
end

-- Каждое чтение вызывающего — обращение: get, has и pull считаются
-- по итогу, отказ драйвера — отдельным словом, а не промахом.
g.test_reads_are_counted_by_what_they_found = function()
    local s = store({ name = 'pages' })

    t.assert_equals(s:get('a'), nil)
    s:put('a', 'значение')
    t.assert_equals(s:get('a'), 'значение')
    t.assert_equals(s:has('a'), true)
    t.assert_equals(s:has('b'), false)
    t.assert_equals(s:pull('a'), 'значение')
    t.assert_equals(s:pull('a', 'нет'), 'нет')

    t.assert_equals(outcomes('pages'), { hit = 3, miss = 3 })

    s.driver.read = function()
        error('связь потеряна', 0)
    end

    t.assert_equals(s:get('a', 'умолчание'), 'умолчание')
    t.assert_equals(s:has('a'), false)
    t.assert_equals(outcomes('pages'), { hit = 3, miss = 3, failed = 2 })
    t.assert_equals(measured('pages'), 8)
    t.assert_equals(
        #helper.journal.find_all('отказал на чтении'),
        1,
        'отказ пишет журнал, повтор подавлен'
    )
end

-- Просроченное — промах, как и отсутствующее: вызывающему значения нет.
g.test_an_expired_value_is_a_miss = function()
    local s = store({ name = 'short' })

    s:put('a', 'значение', 1)
    clock.advance(2)

    t.assert_equals(s:get('a'), nil)
    t.assert_equals(outcomes('short'), { miss = 1 })
end

-- pull у драйвера, который читает и стирает одним действием, считается
-- так же: обращение то же, чтение — его `take`.
g.test_pull_through_the_take_of_the_driver_is_counted = function()
    local inner = cache.memory.new()
    local driver = setmetatable({
        take = function(key)
            local value, expires_at = inner.read(key)

            inner.erase(key)

            return value, expires_at
        end,
    }, { __index = inner })
    local s = cache.new({ driver = driver, now = clock.realtime })

    s:put('token', 'знак')

    t.assert_equals(s:pull('token'), 'знак')
    t.assert_equals(s:pull('token'), nil)
    t.assert_equals(outcomes('memory'), { hit = 1, miss = 1 })
end

-- Запись и то, что решает по прочитанному, — не обращения за значением:
-- их отказ бросает, и в ряды они не идут.
g.test_writes_and_conditional_writes_are_not_requests = function()
    local s = store({ name = 'writes' })

    s:put('a', 1)
    t.assert_equals(s:add('a', 2), false)
    t.assert_equals(s:add('b', 2), true)
    t.assert_equals(s:increment('a'), 2)
    s:forget('a')

    local lock = s:lock('job', 5)

    t.assert_equals(lock:release(), true)
    s:flush()

    t.assert_equals(outcomes('writes'), {})
    t.assert_equals(measured('writes'), nil)
end

-- remember — одно обращение: промах, после которого значение посчитано,
-- и попадание, которое счёт не зовёт.
g.test_remember_is_one_request = function()
    local s = store({ name = 'remember' })
    local calls = 0

    local function compute()
        calls = calls + 1

        return 'страница'
    end

    t.assert_equals(s:remember('docs', 60, compute), 'страница')
    t.assert_equals(s:remember('docs', 60, compute), 'страница')
    t.assert_equals(s:tags('docs'):remember('index', 60, compute), 'страница')
    t.assert_equals(calls, 2)
    t.assert_equals(outcomes('remember'), { hit = 1, miss = 2 })
end

-- remember с замком — тоже одно обращение: повторное чтение под замком
-- и опрос ожидающего — часть его, а не новые промахи.
g.test_remember_with_a_lock_counts_its_first_read_only = function()
    local s = store({ name = 'locked' })

    t.assert_equals(
        s:remember('docs', 60, function()
            return 'страница'
        end, { lock = 5 }),
        'страница'
    )
    t.assert_equals(outcomes('locked'), { miss = 1 })
    t.assert_equals(measured('locked'), 1)

    t.assert_equals(
        s:remember('docs', 60, function()
            error('считать было не нужно')
        end, { lock = 5 }),
        'страница'
    )
    t.assert_equals(outcomes('locked'), { hit = 1, miss = 1 })

    -- Сосед держит замок и кладёт значение на втором опросе: ожидающий
    -- читал трижды, а обращение одно — промах.
    local holder = s:lock('slow', 5)
    local polls = 0

    cache.store._set_source({
        monotonic = clock.monotonic,
        scheduler_now = clock.scheduler_now,
        sleep = function(seconds)
            clock.sleep(seconds)
            polls = polls + 1

            if polls == 2 then
                s:put('slow', 'соседа')
                holder:release()
            end
        end,
    })

    t.assert_equals(
        s:remember('slow', 60, function()
            error('считает сосед')
        end, { lock = 5, wait = 1 }),
        'соседа'
    )
    t.assert_equals(polls, 2)
    t.assert_equals(outcomes('locked'), { hit = 1, miss = 2 })
    t.assert_equals(measured('locked'), 3)
end

-- Длительность — чтение драйвером по монотонным часам хранилища, у отказа
-- тоже; корзины — от 100 мкс до секунды.
g.test_the_duration_is_the_read_of_the_driver = function()
    local moments = { 10, 10.0003, 20, 20.75, 30, 31.5 }

    cache.store._set_source({
        monotonic = function()
            return table.remove(moments, 1)
        end,
    })

    local s = store({ name = 'timed' })

    s:get('a')
    s:has('a')

    s.driver.read = function()
        error('связь потеряна', 0)
    end

    s:get('a')

    t.assert_equals(#moments, 0, 'часы спрошены дважды на обращение')
    t.assert_equals(measured('timed'), 3)
    t.assert_almost_equals(helper.value('cache_request_duration_seconds_sum', { store = 'timed' }), 2.2503, 1e-9)

    local function bucket(le)
        return helper.value('cache_request_duration_seconds_bucket', { store = 'timed', le = le })
    end

    t.assert_equals(bucket(0.0001), 0)
    t.assert_equals(bucket(0.0005), 1)
    t.assert_equals(bucket(0.5), 1)
    t.assert_equals(bucket(1), 2)
    t.assert_equals(bucket(math.huge), 3)
    t.assert_equals(series.BUCKETS, { 0.0001, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1 })
end

-- Хранилище в метке — по имени; без имени — по имени драйвера.
g.test_the_store_label_is_its_name_or_the_name_of_its_driver = function()
    local named = store({ name = 'sessions', prefix = 'sess:' })
    local unnamed = store()
    local custom = cache.new({ driver = setmetatable({ name = 'свой' }, { __index = cache.memory.new() }) })

    named:get('a')
    unnamed:get('a')
    custom:get('a')

    t.assert_equals(outcomes('sessions'), { miss = 1 })
    t.assert_equals(outcomes('memory'), { miss = 1 })
    t.assert_equals(outcomes('свой'), { miss = 1 })
    t.assert_equals(named:status(), { name = 'sessions', driver = 'memory', prefix = 'sess:', ttl = nil })
    t.assert_equals(custom:status().name, 'свой')
    t.assert_equals(series.STORES, 100)
end

-- Хранилище без имени упало бы на первом чтении, далеко от строки, где
-- его заводили; поэтому имя проверяется при заведении.
g.test_a_store_without_a_name_is_refused = function()
    local refusal =
        'кэш: имя хранилища — непустая строка: name в настройках либо name драйвера'
    local nameless = setmetatable({ name = false }, { __index = cache.memory.new() })

    t.assert_error_msg_equals(refusal, cache.new, { name = '' })
    t.assert_error_msg_equals(refusal, cache.new, { name = 7 })
    t.assert_error_msg_equals(refusal, cache.new, { driver = nameless })
    t.assert_equals(cache.new({ name = 'своё', driver = nameless }):status().name, 'своё')
end
