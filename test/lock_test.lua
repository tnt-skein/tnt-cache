--- Тесты замка на ключ и `remember` с замком: взятие, снятие своего,
--- ожидание по часам-двойнику, счёт одним на всех.
---
--- Ожидание идёт по часам из оснастки: пауза записывается и двигает часы,
--- и проверка видит каждый сон ожидающего, а идёт мгновенно. Настоящие
--- файберы и настоящий сон — в последней проверке и в живых проверках
--- спейса и Redis.

local t = require('luatest')

local helper = dofile('test/helper.lua')

local g = t.group('tnt.cache.lock')

---@type any
local cache

---@type table
local clock

--- Что сделать на очередной паузе: номер паузы → действие.
---@type table<integer, fun()>
local on_sleep

--- Подмена внешних средств хранилища: часы-двойник, пауза с действием
--- и метки по порядку. Проверка дописывает в неё своё — признак
--- транзакции, — а не заменяет: новая подмена сбросила бы прежнюю.
---@type table
local sources

--- Сколько пауз проверка терпит: сломанное условие выхода крутило бы
--- ожидание вхолостую, а часы-двойник не дали бы ему упереться в срок.
local MAX_SLEEPS = 50

g.before_each(function()
    cache = helper.fresh()
    clock = helper.clock()
    on_sleep = {}

    local issued = 0

    sources = {
        monotonic = clock.monotonic,
        scheduler_now = clock.scheduler_now,
        sleep = function(seconds)
            clock.sleep(seconds)

            if #clock.slept > MAX_SLEEPS then
                error('проверка зациклилась: ожидание не кончается')
            end

            local action = on_sleep[#clock.slept]

            if action ~= nil then
                action()
            end
        end,
        token = function()
            issued = issued + 1

            return 'метка-' .. issued
        end,
    }

    cache.store._set_source(sources)
end)

g.after_each(helper.unload)

--- Хранилище с проверочными часами.
---@param opts table|nil
---@return any
local function store(opts)
    return helper.store(cache, clock, opts)
end

--- Внутри транзакции box — с этого мига.
local function in_transaction()
    sources.in_transaction = function()
        return true
    end

    cache.store._set_source(sources)
end

-- ── Замок ────────────────────────────────────────────────────────────

-- Замок достаётся одному; лежит под своей меткой, а не под ключом
-- значения; снимает его только взявший, и снятый берут снова.
g.test_lock_is_taken_once_and_released_by_its_holder = function()
    local s = store({ prefix = 'p:' })
    local lock, err = s:lock('report', 10)

    t.assert_equals(err, nil)
    t.assert_equals({ lock.key, lock.full, lock.token }, { 'report', 'p:\0lock:report', 'метка-1' })
    t.assert_equals({ s.driver.read('p:\0lock:report') }, { 'метка-1', clock.realtime() + 10 })
    t.assert_equals({ s:lock('report', 10) }, { nil, 'кэш: замок report занят' })
    t.assert_equals(s:get('report'), nil, 'замок не значение того же ключа')

    s:put('report', 'отчёт')

    t.assert_equals(lock:release(), true)
    t.assert_equals(lock:release(), false, 'снятый второй раз уже не наш')
    t.assert_equals(s:get('report'), 'отчёт', 'снятие замка значения не трогает')

    local again = s:lock('report', 0.5)

    t.assert_equals(again.token, 'метка-3')
    t.assert_equals(clock.slept, {}, 'без wait не ждут')
end

-- Метка держателя — настоящий UUID, у каждого взятия свой: с одной
-- меткой на всех прежний держатель снял бы замок нового.
g.test_the_holder_mark_is_a_fresh_uuid = function()
    cache.store._set_source(nil)

    local s = store()
    local first = s:lock('report', 10)

    clock.advance(10)

    local second = s:lock('report', 10)

    t.assert_str_matches(first.token, '%x+%-%x+%-%x+%-%x+%-%x+')
    t.assert_not_equals(first.token, second.token)
    t.assert_equals(first:release(), false)
    t.assert_equals(second:release(), true)
end

-- Замок со сроком уходит сам; прежний держатель чужой замок не снимет.
g.test_an_expired_holder_cannot_release_the_next_one = function()
    local s = store()
    local first = s:lock('report', 10)

    clock.advance(10)

    local second = s:lock('report', 10)

    t.assert_not_equals(second, nil, 'срок вышел — замок свободен')
    t.assert_equals(first:release(), false)
    t.assert_equals({ s:lock('report', 10) }, { nil, 'кэш: замок report занят' })
    t.assert_equals(second:release(), true)

    clock.advance(10)

    t.assert_equals(second:release(), false, 'пустой ключ — снимать нечего')
end

-- Ожидающий спрашивает раз в POLL и берёт замок, как только его сняли.
g.test_lock_waits_until_the_holder_releases = function()
    local s = store()
    local holder = s:lock('report', 10)

    on_sleep[2] = function()
        holder:release()
    end

    local lock, err = s:lock('report', 10, { wait = 1 })

    t.assert_equals(err, nil)
    t.assert_equals(lock.token, 'метка-4')
    t.assert_equals(clock.slept, { 0.05, 0.05 })
    t.assert_equals(cache.store.POLL, 0.05)
end

-- Ожидание кончается ровно в срок: последний сон — остаток, а не POLL.
g.test_lock_wait_ends_on_its_deadline = function()
    local s = store()

    s:lock('report', 10)

    t.assert_equals({ s:lock('report', 10, { wait = 0.12 }) }, { nil, 'кэш: замок report занят' })
    t.assert_almost_equals(clock.slept[3], 0.02, 1e-9)
    t.assert_equals({ clock.slept[1], clock.slept[2], #clock.slept }, { 0.05, 0.05, 3 })
end

-- Срок вышел, пока шла попытка, — спать дальше нечего.
g.test_no_sleep_after_the_deadline_passed_during_an_attempt = function()
    local s = store()
    local read = s.driver.read

    s:lock('report', 10)

    s.driver.read = function(key)
        clock.advance(1)

        return read(key)
    end

    t.assert_equals({ s:lock('report', 10, { wait = 0.5 }) }, { nil, 'кэш: замок report занят' })
    t.assert_equals(clock.slept, {})
end

-- Остаток сна считается от отметки цикла: она отстаёт после работы без
-- уступки, и сон по ней кончается в тот же миг, что и срок.
g.test_the_last_sleep_is_measured_from_the_scheduler_mark = function()
    local s = store()

    s:lock('report', 10)
    clock.lag = 0.03

    t.assert_equals({ s:lock('report', 10, { wait = 0.02 }) }, { nil, 'кэш: замок report занят' })
    t.assert_almost_equals(clock.slept[1], 0.05, 1e-9)
    t.assert_equals(#clock.slept, 1)
end

-- Замок берут через add драйвера и снимают его erase_if, если они есть:
-- у драйвера по сети оба — одно действие сервера.
g.test_lock_uses_the_one_step_actions_of_the_driver = function()
    local calls = {}
    local inner = cache.memory.new()
    local driver = setmetatable({
        add = function(key, value, expires_at, tags)
            table.insert(calls, { 'add', key, value, expires_at, tags })

            return true
        end,
        erase_if = function(key, value)
            table.insert(calls, { 'erase_if', key, value })

            return true
        end,
        read = function()
            error('читать было нечего', 0)
        end,
    }, { __index = inner })

    local s = store({ driver = driver, prefix = 'p:' })
    local lock = s:lock('report', 10)

    t.assert_equals(lock:release(), true)
    t.assert_equals(calls, {
        { 'add', 'p:\0lock:report', 'метка-1', clock.realtime() + 10 },
        { 'erase_if', 'p:\0lock:report', 'метка-1' },
    })

    driver.erase_if = function()
        return false
    end

    t.assert_equals(lock:release(), false)
end

-- Замок на спейсе берётся вставкой: занятый ключ — отказ вставки,
-- просроченный стирается чтением, и вставка проходит.
g.test_space_lock_is_taken_by_insert = function()
    cache.space._set_source({
        atomic = function(fn, ...)
            return fn(...)
        end,
    })

    local space = helper.fake_space()
    local s = store({ driver = cache.space.new(space) })
    local inserted = 0
    local insert = space.insert

    space.insert = function(...)
        inserted = inserted + 1

        return insert(...)
    end

    local lock = s:lock('report', 10)

    t.assert_equals(space:get('\0lock:report').value, 'метка-1')
    t.assert_equals({ s:lock('report', 10) }, { nil, 'кэш: замок report занят' })

    clock.advance(10)

    t.assert_not_equals(s:lock('report', 10), nil)
    t.assert_equals(inserted, 2, 'занятый живой замок до вставки не доходит')
    t.assert_equals(lock:release(), false)
end

g.test_lock_refusals = function()
    local s = store()
    local bad_ttl = 'кэш: срок замка — конечное число секунд больше нуля: '
        .. 'замок без срока, брошенный упавшим держателем, не освободится никогда'
    local bad_wait = 'кэш: wait — конечное число секунд не меньше нуля'

    for _, ttl in ipairs({ 0, -1, math.huge, 0 / 0, 'скоро' }) do
        t.assert_error_msg_equals(bad_ttl, s.lock, s, 'report', ttl)
    end

    t.assert_error_msg_equals(bad_ttl, s.lock, s, 'report')

    for _, wait in ipairs({ -1, -0.5, math.huge, 0 / 0, 'долго' }) do
        t.assert_error_msg_equals(bad_wait, s.lock, s, 'report', 10, { wait = wait })
    end

    t.assert_error_msg_equals(
        'кэш: настройки lock должны быть таблицей',
        s.lock,
        s,
        'report',
        10,
        5
    )
    t.assert_error_msg_equals(
        'кэш: ключ должен быть непустой строкой',
        s.lock,
        s,
        '',
        10
    )

    local lock = s:lock('report', 10, { wait = 0 })

    t.assert_not_equals(lock, nil, 'wait = 0 — не ждать')
    t.assert_equals(clock.slept, {})
end

-- Ожидание уступает управление — в транзакции его не зовут; без ожидания
-- замок в транзакции берут: взятие — одна запись.
g.test_waiting_for_a_lock_is_refused_inside_a_transaction = function()
    local s = store()
    local refused =
        'кэш: ожидание замка уступает управление — внутри транзакции ждут только с wait = 0'

    in_transaction()

    t.assert_error_msg_equals(refused, s.lock, s, 'report', 10, { wait = 0.5 })
    t.assert_error_msg_equals(refused, s.lock, s, 'report', 10, { wait = 2 })

    local lock = s:lock('report', 10, { wait = 0 })

    t.assert_equals(lock:release(), true)
    t.assert_equals(s:lock('report', 10):release(), true)
end

-- Драйвер по сети внутри транзакции не зовут ни для взятия, ни для снятия.
g.test_remote_lock_is_refused_inside_a_transaction = function()
    local client = helper.fake_redis({ { 'OK' } })
    local s = store({ driver = 'redis', redis = client })
    local lock = s:lock('report', 10)
    local refused = 'кэш: драйвер redis ходит по сети — внутри транзакции box его не зовут: '
        .. 'уступка порвёт транзакцию, а записанное в него при откате останется'

    in_transaction()

    t.assert_error_msg_equals(refused, s.lock, s, 'report', 10)
    t.assert_error_msg_equals(refused, lock.release, lock)
    t.assert_equals(
        #client.calls,
        1,
        'к серверу ходили только за замком вне транзакции'
    )
end

-- Отказ драйвера на взятии бросает: промах значил бы «замок свободен».
g.test_driver_failure_on_lock_throws = function()
    local s = store()

    s.driver.read = function()
        error('связи с кэшем нет', 0)
    end

    t.assert_error_msg_equals('связи с кэшем нет', s.lock, s, 'report', 10)
    t.assert_equals(helper.journal.records(), {}, 'бросок — не промах')
end

-- ── remember с замком ────────────────────────────────────────────────

--- Счёт со счётчиком вызовов.
---@param value any
---@return fun(): any compute
---@return { count: integer } calls
local function counting(value)
    local calls = { count = 0 }

    return function()
        calls.count = calls.count + 1

        return value
    end, calls
end

-- Промах берёт замок, считает, кладёт и снимает замок; попадание
-- не берёт ничего.
g.test_remember_with_a_lock_computes_under_it = function()
    local s = store({ prefix = 'p:' })
    local compute, calls = counting('страница')
    local seen

    local observed = function()
        seen = s.driver.read('p:\0lock:docs')

        return compute()
    end

    t.assert_equals({ s:remember('docs', 60, observed, { lock = 5 }) }, { 'страница' })
    t.assert_equals(seen, 'метка-1', 'считают под замком')
    t.assert_equals({ s.driver.read('p:\0lock:docs') }, {}, 'после счёта замок снят')
    t.assert_equals({ s.driver.read('p:docs') }, { 'страница', clock.realtime() + 60 })
    t.assert_equals(s:remember('docs', 60, observed, { lock = 5 }), 'страница')
    t.assert_equals(calls.count, 1)
end

-- Взявший замок читает кэш ещё раз: сосед досчитал между промахом
-- и замком — второй раз не считают.
g.test_the_holder_reads_again_under_the_lock = function()
    local s = store()
    local compute, calls = counting('своё')
    local read = s.driver.read
    local first = true

    s.driver.read = function(key)
        local value, expires_at = read(key)

        if key == 'docs' and first then
            first = false
            s:put('docs', 'соседское')
        end

        return value, expires_at
    end

    t.assert_equals(s:remember('docs', 60, compute, { lock = 5 }), 'соседское')
    t.assert_equals(calls.count, 0)
    t.assert_equals({ s.driver.read('\0lock:docs') }, {}, 'замок снят и без счёта')
end

-- Ожидающий получает значение, положенное держателем, и сам не считает.
g.test_a_waiter_gets_the_value_of_the_holder = function()
    local s = store()
    local compute, calls = counting('своё')
    local holder = s:lock('docs', 5)

    on_sleep[2] = function()
        s:put('docs', 'держателя')
        holder:release()
    end

    t.assert_equals(s:remember('docs', 60, compute, { lock = 5, wait = 1 }), 'держателя')
    t.assert_equals(calls.count, 0)
    t.assert_equals(clock.slept, { 0.05, 0.05 })
end

-- Держатель ушёл без значения — замок берёт ожидающий и считает сам.
g.test_a_waiter_takes_over_when_the_holder_leaves_empty_handed = function()
    local s = store()
    local compute, calls = counting('своё')
    local holder = s:lock('docs', 5)

    on_sleep[1] = function()
        holder:release()
    end

    t.assert_equals(s:remember('docs', 60, compute, { lock = 5 }), 'своё')
    t.assert_equals(calls.count, 1)
    t.assert_equals(clock.slept, { 0.05 })
end

-- Не дождались — пара, а не бросок; по умолчанию ждут срок замка.
g.test_a_waiter_gives_up_after_wait = function()
    local s = store()
    local compute, calls = counting('своё')

    s:lock('docs', 5)

    t.assert_equals(
        { s:remember('docs', 60, compute, { lock = 5, wait = 0.12 }) },
        { nil, 'кэш: docs считает сосед — значения не дождались за 0.12 с' }
    )
    t.assert_equals(#clock.slept, 3)

    local waited = #clock.slept

    t.assert_equals(
        { s:remember('docs', 60, compute, { lock = 0.1 }) },
        { nil, 'кэш: docs считает сосед — значения не дождались за 0.1 с' }
    )

    local slept = 0

    for index = waited + 1, #clock.slept do
        slept = slept + clock.slept[index]
    end

    t.assert_almost_equals(slept, 0.1, 1e-9, 'по умолчанию ждут срок замка')
    t.assert_equals(calls.count, 0)
    t.assert_equals(
        { s:remember('docs', 60, compute, { lock = 5, wait = 0 }) },
        { nil, 'кэш: docs считает сосед — значения не дождались за 0 с' }
    )
end

-- Поломка счёта уходит как есть, но замок снимается: ожидающим не ждать
-- его срока.
g.test_a_failing_compute_releases_the_lock = function()
    local s = store()
    local failure = { message = 'хранилище данных недоступно' }

    local ok, err = pcall(s.remember, s, 'docs', 60, function()
        error(failure)
    end, { lock = 5 })

    t.assert_equals(ok, false)
    t.assert_is(err, failure)
    t.assert_equals({ s.driver.read('\0lock:docs') }, {})
    t.assert_equals(s:get('docs'), nil)
end

-- Настройки проверяются и при попадании: ошибка программиста бросает
-- всегда, а не на первом промахе.
g.test_remember_options_are_checked_even_on_a_hit = function()
    local s = store()

    s:put('docs', 'есть')

    t.assert_error_msg_equals(
        'кэш: настройки remember — таблица { lock, wait }',
        s.remember,
        s,
        'docs',
        60,
        tostring,
        5
    )
    t.assert_error_msg_equals(
        'кэш: срок замка — конечное число секунд больше нуля: '
            .. 'замок без срока, брошенный упавшим держателем, не освободится никогда',
        s.remember,
        s,
        'docs',
        60,
        tostring,
        {}
    )
    t.assert_error_msg_equals(
        'кэш: wait — конечное число секунд не меньше нуля',
        s.remember,
        s,
        'docs',
        60,
        tostring,
        { lock = 5, wait = -1 }
    )

    in_transaction()

    t.assert_error_msg_equals(
        'кэш: ожидание замка уступает управление — внутри транзакции ждут только с wait = 0',
        s.remember,
        s,
        'docs',
        60,
        tostring,
        { lock = 5 }
    )
    t.assert_equals(s:remember('docs', 60, tostring, { lock = 5, wait = 0 }), 'есть')
end

-- Вид с тегами считает с замком так же и помечает посчитанное.
g.test_tagged_remember_with_a_lock_marks_the_value = function()
    local s = store()
    local compute, calls = counting('страница')
    local docs = s:tags('docs')

    t.assert_equals(docs:remember('docs:index', 60, compute, { lock = 5 }), 'страница')
    t.assert_equals(docs:flush(), 1)
    t.assert_equals(calls.count, 1)
end

-- Настоящие файберы и настоящий сон: десять промахов разом — один счёт.
g.test_ten_fibers_compute_once = function()
    cache.store._set_source(nil)

    local calls, got = helper.stampede(cache.new({ driver = 'memory' }), 'docs')

    t.assert_equals({ calls, #got, got[1], got[10] }, { 1, 10, 'страница', 'страница' })
end
