--- Тесты тегов: пометка записи, забывание тега кусками, указатель
--- «тег → ключи» у драйвера в памяти и на спейсе, доводка спейса.
---
--- Настоящий спейс с индексом по элементам массива — `space_live_test.lua`,
--- настоящий Redis — `redis_live_test.lua`.

local t = require('luatest')

local helper = dofile('test/helper.lua')

local g = t.group('tnt.cache.tags')

---@type any
local cache

---@type table
local clock

--- Хранилище над данным драйвером с проверочными часами.
---@param driver table|nil По умолчанию — в памяти
---@param prefix string|nil
---@return any
local function store(driver, prefix)
    return helper.store(cache, clock, { driver = driver or cache.memory.new(), prefix = prefix })
end

--- Двойники спейса кэша и спейса тегов; транзакция — вызов со счётом.
---@return table space
---@return table tags
---@return table<string, table<string, boolean>> rows Строки тегов: тег → ключи
---@return { count: integer } transactions
local function fake_spaces()
    local transactions = { count = 0 }

    cache.space._set_source({
        atomic = function(fn, ...)
            transactions.count = transactions.count + 1

            return fn(...)
        end,
    })

    local tags, rows = helper.fake_tags()

    return helper.fake_space(), tags, rows, transactions
end

--- Хранилище над двойниками спейса кэша и спейса тегов.
---@return any store
---@return table space
---@return table<string, table<string, boolean>> rows
local function space_store()
    local space, tags, rows = fake_spaces()

    return store(cache.space.new(space, tags)), space, rows
end

g.before_each(function()
    cache = helper.fresh()
    clock = helper.clock()
end)

g.after_each(helper.unload)

--- Драйвер поверх данного, который пишет, с какими тегами его зовут.
---@param inner table
---@return table driver
---@return table[] calls `{ действие, ключ, теги }` по порядку
local function spying(inner)
    local calls = {}

    local driver = setmetatable({
        write = function(key, value, expires_at, tags)
            table.insert(calls, { 'write', key, tags })

            return inner.write(key, value, expires_at, tags)
        end,
        erase_tagged = function(tag, limit)
            table.insert(calls, { 'erase_tagged', tag, limit })

            return inner.erase_tagged(tag, limit)
        end,
    }, { __index = inner })

    return driver, calls
end

--- Одна и та же история на любом хранилище: две записи с общим тегом,
--- третья с другим и четвёртая без тегов; забывание тега убирает ровно две.
---@param s any
local function forgets_only_its_own(s)
    local docs = s:tags({ 'docs' })

    docs:put('docs:1', 'первая', 60)
    s:tags({ 'lists', 'docs' }):put('docs:index', { 1 })
    s:tags('news'):put('news:1', 'новость')
    s:put('plain', 'без тегов')

    t.assert_equals(s:get('docs:1'), 'первая', 'запись с тегом читается по ключу')
    t.assert_equals(docs:flush(), 2)
    t.assert_equals(s:get('docs:1'), nil)
    t.assert_equals(s:get('docs:index'), nil, 'второй тег записи не держит её')
    t.assert_equals(s:get('news:1'), 'новость')
    t.assert_equals(s:get('plain'), 'без тегов')
    t.assert_equals(
        s:tags('lists'):flush(),
        0,
        'стёртая запись вынута и из прочих тегов'
    )
    t.assert_equals(docs:flush(), 0, 'забытый тег пуст')
end

-- ── Хранилище ────────────────────────────────────────────────────────

g.test_tag_flush_forgets_only_its_entries_in_memory = function()
    forgets_only_its_own(store())
end

g.test_tag_flush_forgets_only_its_entries_in_a_space = function()
    local s, space, rows = space_store()

    forgets_only_its_own(s)

    t.assert_equals(space:get('news:1').tags, { 'news' })
    t.assert_equals(rows, { news = { ['news:1'] = true } }, 'строки забытых тегов стёрты')
end

-- Теги — с приставкой хранилища: соседи на одном драйвере своих тегов
-- не делят.
g.test_tags_carry_the_store_prefix = function()
    local driver, calls = spying(cache.memory.new())
    local left = store(driver, 'l:')
    local right = store(driver, 'r:')

    left:tags('docs'):put('a', 1)
    right:tags('docs'):put('a', 2)

    t.assert_equals(left:tags('docs'):flush(), 1)
    t.assert_equals(left:get('a'), nil)
    t.assert_equals(right:get('a'), 2, 'тег соседа с другой приставкой цел')
    t.assert_equals(calls, {
        { 'write', 'l:a', { 'l:docs' } },
        { 'write', 'r:a', { 'r:docs' } },
        { 'erase_tagged', 'l:docs', 1000 },
    })
end

-- Тег — строка либо список; повторы отбрасываются до драйвера.
g.test_tags_take_a_name_or_a_list_without_repeats = function()
    local driver, calls = spying(cache.memory.new())
    local s = store(driver, 'p:')

    s:tags('docs'):put('a', 1)
    s:tags({ 'b', 'a', 'b' }):put('b', 2)

    t.assert_equals(calls, {
        { 'write', 'p:a', { 'p:docs' } },
        { 'write', 'p:b', { 'p:b', 'p:a' } },
    })
end

-- add и remember вида помечают запись так же, как put; remember считает
-- только на промахе.
g.test_tagged_add_and_remember_mark_the_entry = function()
    local s = store()
    local docs = s:tags({ 'docs' })
    local calls = 0

    local compute = function()
        calls = calls + 1

        return 'посчитано'
    end

    t.assert_equals(docs:add('a', 1, 10), true)
    t.assert_equals(docs:add('a', 2, 10), false)
    t.assert_equals(docs:remember('b', 10, compute), 'посчитано')
    t.assert_equals(docs:remember('b', 10, compute), 'посчитано')
    t.assert_equals(calls, 1)
    t.assert_equals(docs:flush(), 2)
    t.assert_equals({ s:get('a'), s:get('b') }, {})

    clock.advance(10)

    t.assert_equals(docs:add('c', 1, 10), true)

    clock.advance(10)

    t.assert_equals(
        docs:add('c', 2),
        true,
        'просроченное — отсутствующее и с тегами'
    )
    t.assert_equals(s:get('c'), 2)
end

-- Перезапись без тега снимает тег у записи памяти и спейса: забывание
-- тега её больше не трогает.
g.test_rewrite_without_a_tag_drops_the_tag = function()
    for _, s in ipairs({ store(), (space_store()) }) do
        s:tags('docs'):put('a', 1)
        s:put('a', 2)

        t.assert_equals(s:tags('docs'):flush(), 0)
        t.assert_equals(s:get('a'), 2)

        s:tags('docs'):put('b', 1)
        s:tags('news'):put('b', 2)

        t.assert_equals(s:tags('docs'):flush(), 0, 'новый тег заменяет прежний')
        t.assert_equals(s:tags('news'):flush(), 1)
    end
end

-- Прибавка меняет значение, а теги записи держит — у памяти и спейса,
-- где прибавку собирает хранилище.
g.test_increment_keeps_the_tags = function()
    for _, s in ipairs({ store(), (space_store()) }) do
        s:tags('hits'):put('n', 1, 10)

        t.assert_equals(s:increment('n', 2), 3)
        t.assert_equals(s:tags('hits'):flush(), 1)
        t.assert_equals(s:get('n'), nil)
    end
end

-- Забывание тега идёт кусками по тысяче, с уступкой перед каждым, тег
-- за тегом; ответ — сколько стёрто за все куски.
g.test_tag_flush_walks_in_chunks_with_a_yield_before_each = function()
    local driver, calls = spying(cache.memory.new())
    local s = store(driver, 'p:')
    local yields = 0
    local docs = s:tags('docs')

    cache.store._set_source({
        yield = function()
            yields = yields + 1
        end,
    })

    for index = 1, 1001 do
        docs:put('k' .. index, index)
    end

    s:tags('news'):put('n', 1)

    local written = #calls

    t.assert_equals(s:tags({ 'docs', 'news' }):flush(), 1002)
    t.assert_equals(yields, 3, 'тег docs — два куска, news — один')
    t.assert_equals({ unpack(calls, written + 1) }, {
        { 'erase_tagged', 'p:docs', 1000 },
        { 'erase_tagged', 'p:docs', 1000 },
        { 'erase_tagged', 'p:news', 1000 },
    })
end

-- Ровно тысяча — ещё один кусок: драйвер не знает, кончился ли тег,
-- пока не отдаст меньше предела.
g.test_a_full_chunk_asks_for_one_more = function()
    local driver, calls = spying(cache.memory.new())
    local s = store(driver)

    for index = 1, 1000 do
        s:tags('docs'):put('k' .. index, index)
    end

    t.assert_equals(s:tags('docs'):flush(), 1000)
    t.assert_equals(#calls, 1002, 'тысяча записей и два куска забывания')
end

g.test_tag_refusals = function()
    local s = store()
    local bad =
        'кэш: теги — непустая строка либо непустой список непустых строк'

    for _, names in ipairs({ '', 7, {}, { '' }, { 1 }, { 'a', false }, { key = 'a' } }) do
        t.assert_error_msg_equals(bad, s.tags, s, names)
    end

    t.assert_error_msg_equals(bad, s.tags, s)

    local custom = cache.new({ driver = setmetatable({ name = 'custom' }, { __index = function() end }) })

    t.assert_error_msg_equals(
        'кэш: драйвер custom тегов не знает: у него нет erase_tagged',
        custom.tags,
        custom,
        'docs'
    )

    local plain = helper.fake_space()

    plain.name = 'app_cache'

    local old = store(cache.space.new(plain))

    t.assert_error_msg_equals(
        'кэш: драйвер space тегов не знает: нет спейса тегов app_cache_tags — его заведёт шаг миграции '
            .. 'tnt.cache.space.migrate',
        old.tags,
        old,
        'docs'
    )

    old:put('a', 1)

    t.assert_equals(old:increment('n'), 1, 'без тегов спейс работает по-прежнему')
    t.assert_equals(old:get('a'), 1)

    cache.store._set_source({
        in_transaction = function()
            return true
        end,
    })

    local docs = s:tags('docs')

    docs:put('a', 1)

    t.assert_error_msg_equals(
        'кэш: забывание тега уступает управление и само фиксирует партии — внутри транзакции его не зовут',
        docs.flush,
        docs
    )
    t.assert_equals(s:get('a'), 1, 'в транзакции ничего не стёрто')
end

-- Отказ драйвера посреди забывания бросает как есть: стёртое до него
-- остаётся стёртым.
g.test_driver_failure_on_tag_flush_throws = function()
    local s = store()
    local driver = s.driver
    local inner = driver.erase_tagged

    s:tags('a'):put('x', 1)
    s:tags('b'):put('y', 2)

    driver.erase_tagged = function(tag, limit)
        if tag == 'b' then
            error('связи с кэшем нет', 0)
        end

        return inner(tag, limit)
    end

    local both = s:tags({ 'a', 'b' })

    t.assert_error_msg_equals('связи с кэшем нет', both.flush, both)
    t.assert_equals(s:get('x'), nil)
    t.assert_equals(s:get('y'), 2)
    t.assert_equals(helper.journal.records(), {}, 'бросок — не промах')
end

-- ── Драйвер в памяти ─────────────────────────────────────────────────

-- Кусок не длиннее предела, и следующий берётся с начала множества:
-- стёртые из него уже вынуты.
g.test_memory_driver_erases_a_tag_by_the_chunk = function()
    local driver = cache.memory.new()

    for index = 1, 3 do
        driver.write('k' .. index, index, 0, { 'docs' })
    end

    driver.write('other', 0, 0, { 'news' })

    t.assert_equals({ driver.erase_tagged('docs', 2) }, { 2, 2 })
    t.assert_equals({ driver.erase_tagged('docs', 2) }, { 1, 1 })
    t.assert_equals({ driver.erase_tagged('docs', 2) }, { 0, 0 })
    t.assert_equals({ driver.erase_tagged('none', 2) }, { 0, 0 })
    t.assert_equals(driver.read('other'), 0)
    t.assert_equals({ driver.read('k1') }, {})
end

-- Всякое стирание вынимает ключ из указателя: одиночное, партией,
-- по тегу соседа и очисткой. Прочие ключи тега остаются в нём.
g.test_memory_driver_keeps_the_tag_index_in_step = function()
    local driver = cache.memory.new()

    for index = 1, 4 do
        driver.write('k' .. index, index, 0, { 'docs', 'all' })
    end

    driver.erase('k1')
    driver.erase('missing')
    driver.erase_many({ 'k2', 'missing' })

    t.assert_equals({ driver.read('k3') }, { 3, 0, { 'docs', 'all' } })
    t.assert_equals({ driver.erase_tagged('docs', 10) }, { 2, 2 })
    t.assert_equals({ driver.erase_tagged('all', 10) }, { 0, 0 })

    driver.write('a', 1, 0, { 'docs' })
    driver.write('b', 2, 0, { 'docs' })
    driver.clear()

    t.assert_equals({ driver.erase_tagged('docs', 10) }, { 0, 0 }, 'очистка сносит и указатель')

    driver.write('c', 3, 0, { 'docs' })

    t.assert_equals({ driver.erase_tagged('docs', 10) }, { 1, 1 })
end

-- Просроченное, стёртое чисткой, вынимается из тега тоже.
g.test_sweep_takes_the_expired_out_of_its_tags = function()
    local s = store()

    s:tags('docs'):put('a', 1, 10)
    s:tags('docs'):put('b', 2, 20)
    clock.advance(10)

    t.assert_equals(s:sweep(), { swept = 1, scanned = 2 })
    t.assert_equals({ s.driver.erase_tagged('docs', 10) }, { 1, 1 })
end

-- ── Драйвер на спейсе ────────────────────────────────────────────────

-- Запись помнит теги четвёртым полем, спейс тегов — строку на тег;
-- перезапись, стирание и партия вынимают прежние строки.
g.test_space_driver_keeps_tag_rows_in_step = function()
    local space, tags, rows = fake_spaces()
    local driver = cache.space.new(space, tags)

    driver.write('a', 1, 0, { 'docs', 'all' })
    driver.write('b', 2, 5, nil)
    driver.write('c', 3, 0, { 'docs' })
    driver.write('d', 4, 0, { 'docs' })

    t.assert_equals({ driver.read('a') }, { 1, 0, { 'docs', 'all' } })
    t.assert_equals({ driver.read('b') }, { 2, 5 })
    t.assert_equals(rows, { docs = { a = true, c = true, d = true }, all = { a = true } })

    driver.write('a', 5, 0, { 'news' })

    t.assert_equals(
        rows,
        { docs = { c = true, d = true }, news = { a = true } },
        'перезапись меняет теги'
    )

    driver.erase('c')
    driver.erase('missing')

    t.assert_equals(rows, { docs = { d = true }, news = { a = true } })

    driver.erase_many({ 'd', 'missing' })

    t.assert_equals(rows, { news = { a = true } })
    t.assert_equals(driver.read('b'), 2)

    driver.clear()

    t.assert_equals({ rows, driver.read('b') }, { {} }, 'очистка сносит и строки тегов')
    t.assert_equals(driver.tagless, nil)
end

-- Забывание — куском строк тега не длиннее предела и одной транзакцией
-- на кусок; строка без записи стирается тоже, но записью не считается.
g.test_space_driver_erases_a_tag_by_the_chunk = function()
    local space, tags, rows, transactions = fake_spaces()
    local driver = cache.space.new(space, tags)

    for index = 1, 3 do
        driver.write('k' .. index, index, 0, { 'docs' })
    end

    driver.write('other', 0, 0, { 'news' })
    space:delete('k3')
    transactions.count = 0

    t.assert_equals({ driver.erase_tagged('docs', 2) }, { 2, 2 })
    t.assert_equals({ driver.erase_tagged('docs', 2) }, { 1, 0 })
    t.assert_equals({ driver.erase_tagged('docs', 2) }, { 0, 0 })
    t.assert_equals(transactions.count, 2, 'пустой кусок транзакции не заводит')
    t.assert_equals(rows, { news = { other = true } })
    t.assert_equals({ driver.read('k1') }, {})
    t.assert_equals(driver.read('other'), 0)
end

-- Вставка: свободный ключ — `true` со строками тегов, занятый — `false`,
-- прочий отказ уходит как есть; без спейса тегов — одной командой.
g.test_space_driver_inserts_only_a_free_key = function()
    local space, tags, rows = fake_spaces()
    local driver = cache.space.new(space, tags)

    t.assert_equals(driver.insert('a', 1, 0, { 'docs' }), true)
    t.assert_equals(driver.insert('a', 2, 0, { 'news' }), false)
    t.assert_equals({ driver.read('a') }, { 1, 0, { 'docs' } })
    t.assert_equals(rows, { docs = { a = true } })

    local plain = cache.space.new(helper.fake_space())

    t.assert_equals(plain.insert('a', 1, 0, nil), true)
    t.assert_equals(plain.insert('a', 2, 0, nil), false)
    t.assert_equals(plain.read('a'), 1)

    local failure = { code = 7, message = 'только чтение' }

    space.insert = function()
        error(failure)
    end

    local ok, err = pcall(driver.insert, 'b', 1, 0, nil)

    t.assert_equals(ok, false)
    t.assert_is(err, failure)
end

-- Драйвер по имени: вид с тегами заводится всегда — до первого обращения
-- неизвестно, есть ли спейс тегов, — но пока спейса тегов нет, запись
-- с тегами и забывание тега отказывают тем же словом, что вид над спейсом
-- без тегов. Спейс тегов, заведённый шагом позже, то же хранилище видит
-- первым обращением, и драйвер с тегами больше спейсов не ищет.
g.test_space_driver_by_name_picks_up_the_tags_space_on_first_use = function()
    local plain = helper.fake_space()
    local spaces, asked = helper.named_spaces(cache)

    plain.name = 'app_cache'
    spaces.app_cache = plain

    local s = store(cache.space.named('app_cache'))
    local docs = s:tags('docs')
    local tagless = 'кэш: драйвер space тегов не знает: нет спейса тегов app_cache_tags — '
        .. 'его заведёт шаг миграции tnt.cache.space.migrate'

    t.assert_error_msg_equals(tagless, docs.put, docs, 'a', 1)
    t.assert_error_msg_equals(tagless, docs.add, docs, 'a', 1)
    t.assert_error_msg_equals(tagless, docs.flush, docs)
    t.assert_equals({ plain:get('a') }, {}, 'тег, который не забыть, не лёг')

    s:put('a', 1)

    t.assert_equals(s:increment('n'), 1, 'без тегов спейс работает по-прежнему')

    -- Шаг довёл спейс: спейс тегов есть, хранилище то же.
    local tags, rows = helper.fake_tags()

    spaces.app_cache_tags = tags
    asked()

    docs:put('b', 2)

    t.assert_equals(docs:add('c', 3), true)
    t.assert_equals(rows, { docs = { b = true, c = true } })
    t.assert_equals(docs:flush(), 2)
    t.assert_equals(s:get('a'), 1)
    t.assert_equals(
        asked(),
        { 'app_cache', 'app_cache_tags' },
        'драйвер с тегами найден раз и навсегда'
    )
end
