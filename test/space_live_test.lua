--- Живая проверка драйвера на спейсе: настоящий box, миграция и обход.

local t = require('luatest')

local g = t.group('tnt.cache.space_live')

local helper = dofile('test/helper.lua')

g.before_all(function()
    g.server = helper.start_node()
end)

g.after_all(function()
    helper.stop_node(g.server)
end)

g.before_each(function()
    -- Исходники грузятся заново на каждую проверку: внешняя зависимость хранилища и спейс
    -- чистые, а прежняя подмена не доживает до соседней проверки.
    g.server:exec(function(modules)
        for _, name in ipairs({ 'app_cache', 'app_cache_tags' }) do
            if box.space[name] ~= nil then
                box.space[name]:drop()
            end
        end

        ---@type any
        local cache = require('tnt.testing.sources').load(modules, 'tnt.cache')

        cache.space.migrate(box)
        cache.space.migrate(box)

        -- Часы двигает проверка: поле таблицы, а не глобал, — строгий
        -- режим и проверка типов глобалы внутри функций не жалуют.
        rawset(_G, 'wall', { now = 1000 })
        rawset(_G, 'cache', cache)
        rawset(
            _G,
            'store',
            cache.new({
                driver = 'space',
                now = function()
                    return _G.wall.now
                end,
            })
        )
    end, { helper.ON_NODE })
end)

g.test_space_cache_survives_in_box_and_sweeps = function()
    local report = g.server:exec(function()
        local store = _G.store

        store:put('a', { id = 7, name = 'Мария' }, 10)
        store:put('b', 'навсегда')
        store:put('c', 1, 5)

        local before = store:get('a')

        _G.wall.now = _G.wall.now + 7

        local swept = store:sweep()
        local rows = box.space.app_cache:count()

        _G.wall.now = _G.wall.now + 10

        return {
            before = before,
            swept = swept,
            rows = rows,
            a_after = store:get('a'),
            b_after = store:get('b'),
            left = box.space.app_cache:count(),
            format = (box.space.app_cache:format()[1] or {}).name,
            primary = box.space.app_cache.index[0].type,
        }
    end)

    t.assert_equals(report.before, { id = 7, name = 'Мария' })
    t.assert_equals(report.swept, { swept = 1, scanned = 3 }, 'c истёк, a и b живы')
    t.assert_equals(report.rows, 2)
    t.assert_equals(report.a_after, nil)
    t.assert_equals(report.b_after, 'навсегда')
    t.assert_equals(report.left, 1)
    t.assert_equals(report.format, 'key')
    t.assert_equals(report.primary, 'TREE')
end

-- Хранилище по имени заводят до шага миграции: сборка спейса не ищет,
-- чтение до шага — промах, запись бросает отказом box «спейса нет»
-- с именем шага, чистка отдаёт пару. Шаг прошёл — то же хранилище
-- пишет, читает и забывает тег.
g.test_a_store_by_name_waits_for_the_migration = function()
    local report = g.server:exec(function()
        local cache = _G.cache

        -- Кодов отказов в аннотациях ядра нет.
        ---@type any
        local errors = box.error

        box.space.app_cache_tags:drop()
        box.space.app_cache:drop()

        local store = cache.new({ driver = 'space' })
        local put = { pcall(store.put, store, 'a', 1) }
        local before = {
            got = store:get('a', 'нет'),
            put = tostring(put[2]),
            no_such_space = put[2].code == errors.NO_SUCH_SPACE,
            swept = select(2, store:sweep()),
        }

        cache.space.migrate(box)
        store:tags('docs'):put('a', 1)

        return { before = before, got = store:get('a'), forgotten = store:tags('docs'):flush() }
    end)

    local missing =
        'кэш: спейса app_cache нет — нужен шаг миграции tnt.cache.space.migrate'

    t.assert_equals(report, {
        before = {
            got = 'нет',
            put = missing,
            no_such_space = true,
            swept = 'кэш: чистка не дошла до конца — стёрто 0, дальше box отказал: '
                .. missing,
        },
        got = 1,
        forgotten = 1,
    })
end

-- Обход настоящего спейса идёт партиями по GT с limit: перед каждым
-- куском уступка, просроченное куска стирается одной транзакцией.
-- Транзакции считаются триггером по box.txn_id.
g.test_sweep_deletes_each_chunk_in_one_transaction = function()
    local report = g.server:exec(function()
        local store = _G.store
        local transactions = {}
        local yields = 0

        _G.cache.store._set_source({
            yield = function()
                yields = yields + 1
            end,
        })

        for index = 1, 2500 do
            store:put(('k%04d'):format(index), index, index % 2 == 0 and 5 or 100)
        end

        box.space.app_cache:on_replace(function()
            transactions[box.txn_id()] = true
        end)

        _G.wall.now = _G.wall.now + 5

        local swept = store:sweep()
        local counted = 0

        for _ in pairs(transactions) do
            counted = counted + 1
        end

        return { swept = swept, transactions = counted, yields = yields, left = box.space.app_cache:count() }
    end)

    t.assert_equals(report.swept, { swept = 1250, scanned = 2500 })
    t.assert_equals(
        report.transactions,
        3,
        'три куска по тысяче — три транзакции на 1250 стираний'
    )
    t.assert_equals(report.yields, 3)
    t.assert_equals(report.left, 1250)
end

-- Обход длиннее среза файбера доходит до конца: срез ставится маленьким,
-- уступка перед каждым куском его обновляет, а обход одним итератором
-- без уступки на нём срывался бы — так же, как настоящий на секунде.
-- Файбер свой и незащищённый, как у такта. Записей с запасом: обход
-- обязан быть длиннее среза втрое, иначе проверка ничего не доказывает.
g.test_sweep_longer_than_the_fiber_slice_completes = function()
    local report = g.server:exec(function()
        local clock = require('clock')
        local fiber = require('fiber')
        local store = _G.store

        for index = 1, 30000 do
            store:put(('k%05d'):format(index), index, 1)
        end

        _G.wall.now = _G.wall.now + 1

        local outcome

        local worker = fiber.new(function()
            ---@diagnostic disable-next-line: undefined-field
            fiber.self():set_max_slice(0.02)

            local started = clock.monotonic()
            local swept = store:sweep({ chunk = 500 })

            outcome = { swept = swept, took = clock.monotonic() - started }
        end)

        worker:set_joinable(true)

        local ok, failure = worker:join()

        return { ok = ok, failure = tostring(failure), outcome = outcome, left = box.space.app_cache:count() }
    end)

    t.assert_equals(report.ok, true, report.failure)
    t.assert_equals(report.outcome.swept, { swept = 30000, scanned = 30000 })
    t.assert_equals(report.left, 0)
    t.assert_gt(
        report.outcome.took,
        0.06,
        'обход короче трёх срезов ничего не доказывает'
    )
end

-- Узел, ставший репликой за уступкой, — отказ парой с числом стёртого
-- до него, а не исключение из глубины box: стёртая партия остаётся
-- стёртой, остаток ждёт следующего обхода.
g.test_replica_midway_is_a_refusal = function()
    local report = g.server:exec(function()
        local store = _G.store
        local yields = 0

        _G.cache.store._set_source({
            yield = function()
                yields = yields + 1

                if yields == 2 then
                    box.cfg({ read_only = true })
                end
            end,
        })

        for index = 1, 3 do
            store:put('k' .. index, index, 1)
        end

        _G.wall.now = _G.wall.now + 1

        local swept = { store:sweep({ chunk = 2 }) }

        box.cfg({ read_only = false })

        return { swept = swept, left = box.space.app_cache:count() }
    end)

    t.assert_equals(report.swept, {
        nil,
        "кэш: чистка не дошла до конца — стёрто 2, дальше box отказал: Can't modify data on a read-only instance "
            .. '- box.cfg.read_only is true',
    })
    t.assert_equals(report.left, 1)
end

-- Реплика только для чтения: просроченная запись читается как
-- отсутствующая и остаётся лежать. Стирать её там нечем — `delete`
-- отказал бы, как всякая запись в спейс, — и чтение падало бы ровно
-- тогда, когда у записи вышел срок. На ведущем она стирается по дороге,
-- как и прежде. Живое чтение при этом работает, а запись отказывает.
g.test_read_only_replica_reads_the_expired_as_a_miss = function()
    local report = g.server:exec(function()
        local store = _G.store
        local attempts = 0
        local erase = store.driver.erase

        -- Стирание считается у самого драйвера: «на реплике не пробовали»
        -- иначе не отличить от «попробовали и отказ проглотили».
        store.driver.erase = function(key)
            attempts = attempts + 1

            return erase(key)
        end

        store:put('a', { id = 7 }, 10)
        store:put('b', 'навсегда')

        _G.wall.now = _G.wall.now + 10

        box.cfg({ read_only = true })

        local expired = store:get('a', 'нет')
        local present = store:has('a')
        local alive = store:get('b')
        local ok, failure = pcall(store.put, store, 'c', 1)
        local tried = attempts
        local left = box.space.app_cache:count()

        box.cfg({ read_only = false })

        return {
            expired = expired,
            present = present,
            alive = alive,
            put = { ok, tostring(failure) },
            tried = tried,
            left = left,
            master = store:get('a', 'нет'),
            erased = attempts,
            rows = box.space.app_cache:count(),
        }
    end)

    t.assert_equals(
        report.expired,
        'нет',
        'просроченное — отсутствующее, а не бросок'
    )
    t.assert_equals(report.present, false)
    t.assert_equals(report.alive, 'навсегда', 'живое читается и на реплике')
    t.assert_equals(report.put, {
        false,
        "Can't modify data on a read-only instance - box.cfg.read_only is true",
    }, 'запись на реплике по-прежнему бросает')
    t.assert_equals(report.tried, 0, 'на реплике стирать не пробуют')
    t.assert_equals(report.left, 2, 'просроченное осталось ведущему и sweep')
    t.assert_equals(report.master, 'нет')
    t.assert_equals(report.erased, 1, 'на ведущем оно стирается по дороге')
    t.assert_equals(report.rows, 1)
end

-- Уступка обрывает транзакцию memtx: изнутри транзакции обход отказывает
-- сразу, а не на фиксации.
g.test_sweep_inside_a_transaction_is_a_programmer_error = function()
    local report = g.server:exec(function()
        local store = _G.store

        store:put('a', 1, 1)

        local ok, err = pcall(box.atomic, function()
            store:sweep()
        end)

        return { ok = ok, err = tostring(err), in_txn = box.is_in_txn() }
    end)

    t.assert_equals(report.ok, false)
    t.assert_equals(
        report.err,
        'кэш: sweep уступает управление и сам фиксирует партии — внутри транзакции его не зовут'
    )
    t.assert_equals(report.in_txn, false)
end

-- Две записи с общим тегом забываются разом, третья с другим тегом
-- и четвёртая без тегов — целы; индекс тегов ядро держит само.
g.test_tag_flush_on_a_live_node = function()
    local report = g.server:exec(function()
        local store = _G.store

        store:tags('docs'):put('docs:1', 'первая', 60)
        store:tags({ 'lists', 'docs' }):put('docs:index', { 1, 2 })
        store:tags('news'):put('news:1', 'новость')
        store:put('plain', 'без тегов')

        local forgotten = store:tags('docs'):flush()

        ---@type any
        local plain = box.space.app_cache:get('plain')

        return {
            forgotten = forgotten,
            docs = { store:get('docs:1'), store:get('docs:index') },
            news = store:get('news:1'),
            plain = store:get('plain'),
            lists = box.space.app_cache_tags:count({ 'lists' }),
            plain_tags = plain.tags,
        }
    end)

    t.assert_equals(report.forgotten, 2)
    t.assert_equals(report.docs, {})
    t.assert_equals(report.news, 'новость')
    t.assert_equals(report.plain, 'без тегов')
    t.assert_equals(report.lists, 0, 'стёртая запись вынута из всех своих тегов')
    t.assert_equals(report.plain_tags, nil, 'запись без тегов — тройка')
end

-- Десять промахов разом на одном ключе — один счёт, остальные ждут
-- и получают посчитанное.
g.test_remember_with_a_lock_computes_once_on_a_live_node = function()
    local report = g.server:exec(function()
        local fiber = require('fiber')
        local store = _G.store
        local calls = 0
        local done = fiber.channel(10)

        -- Итоги собирает канал: функцию помощника на узел не передать,
        -- а ждать каждого файбера по отдельности незачем.
        local function render()
            calls = calls + 1
            fiber.sleep(0.1)

            return 'страница'
        end

        for _ = 1, 10 do
            fiber.create(function()
                done:put(store:remember('page', 60, render, { lock = 5 }))
            end)
        end

        local got = {}

        for index = 1, 10 do
            got[index] = done:get(5)
        end

        return { calls = calls, got = got, locks = box.space.app_cache:count() }
    end)

    t.assert_equals(report.calls, 1)
    t.assert_equals(#report.got, 10)
    t.assert_equals(report.got[10], 'страница')
    t.assert_equals(report.locks, 1, 'в спейсе только значение: замок снят')
end

-- Запись в кэш из транзакции прикладного кода — её часть: откат уносит
-- и запись, и строки её тегов, фиксация кладёт их вместе с данными.
g.test_tagged_write_joins_the_callers_transaction = function()
    local report = g.server:exec(function()
        local store = _G.store

        box.begin()
        store:tags('docs'):put('a', 1)

        local inside = box.space.app_cache_tags:count()

        box.rollback()

        local rolled = { box.space.app_cache:count(), box.space.app_cache_tags:count() }

        box.atomic(function()
            store:tags('docs'):put('b', 2)
        end)

        return { inside = inside, rolled = rolled, kept = { store:get('b'), box.space.app_cache_tags:count() } }
    end)

    t.assert_equals(report, { inside = 1, rolled = { 0, 0 }, kept = { 2, 1 } })
end

-- Спейс, заведённый до тегов, доводится тем же шагом миграции
-- в транзакции, как у раннера: дописывается пустой спейс тегов, а записи
-- остаются. До доводки хранилище работает без тегов: по имени спейса
-- отказывает запись с тегом, над самим спейсом — уже вид с тегами.
-- После доводки хранилище по имени пишет теги без пересборки.
g.test_migrate_adds_tags_to_an_old_space_in_a_transaction = function()
    local report = g.server:exec(function()
        local cache = _G.cache
        local old = box.schema.space.create('old_cache', {
            format = {
                { name = 'key', type = 'string' },
                { name = 'value', type = 'any' },
                { name = 'expires_at', type = 'number' },
            },
        })

        old:create_index('primary', { parts = { 'key' } })

        for index = 1, 100 do
            old:replace({ 'k' .. index, index, 0 })
        end

        local store = cache.new({ driver = 'space', space = 'old_cache' })
        local docs = store:tags('docs')
        local tagless = { pcall(docs.put, docs, 'a', 1) }
        local pinned = cache.new({ driver = 'space', space = old })
        local refused = { pcall(pinned.tags, pinned, 'docs') }

        box.atomic(cache.space.migrate, box, 'old_cache')
        box.atomic(cache.space.migrate, box, 'old_cache')

        -- Хранилище то же, что до доводки: спейс тегов оно находит
        -- первым обращением после шага.
        store:tags('docs'):put('a', 1)
        store:tags('docs'):put('k1', 'переписано')

        ---@type any
        local k1 = old:get('k1')

        local result = {
            tagless = tostring(tagless[2]),
            refused = tostring(refused[2]),
            kept = store:get('k2'),
            tags_of_k1 = k1[4],
            forgotten = store:tags('docs'):flush(),
            rows = old:count(),
            fields = #old:format(),
        }

        box.space.old_cache_tags:drop()
        old:drop()

        return result
    end)

    local tagless = 'кэш: драйвер space тегов не знает: нет спейса тегов old_cache_tags — '
        .. 'его заведёт шаг миграции tnt.cache.space.migrate'

    t.assert_equals(report.tagless, tagless)
    t.assert_equals(report.refused, tagless, 'слово одно, отказывай запись или вид')
    t.assert_equals(report.kept, 2, 'записи пережили доводку')
    t.assert_equals(
        report.tags_of_k1,
        { 'docs' },
        'теги — четвёртым полем и без его имени в формате'
    )
    t.assert_equals(report.forgotten, 2)
    t.assert_equals(report.rows, 99, 'сто прежних и a, без двух забытых')
    t.assert_equals(report.fields, 3, 'вид старого спейса не тронут')
end

-- Узел с MVCC: чтение не видит чужой вставки, пока та пишется в WAL, —
-- замок и `add` берутся вставкой, и достаются одному из пяти. Индекса
-- по элементам массива такой узел не заводит, и теги держит спейс тегов.
local mvcc = t.group('tnt.cache.space_live_mvcc')

mvcc.before_all(function()
    mvcc.server = helper.start_node({ memtx_use_mvcc_engine = true })
end)

mvcc.after_all(function()
    helper.stop_node(mvcc.server)
end)

mvcc.test_the_lock_goes_to_one_of_five = function()
    local report = mvcc.server:exec(function(modules)
        local fiber = require('fiber')

        ---@type any
        local cache = require('tnt.testing.sources').load(modules, 'tnt.cache')

        cache.space.migrate(box)

        local store = cache.new({ driver = 'space' })
        local locks, added = 0, 0
        local fibers = {}

        for index = 1, 5 do
            local worker = fiber.new(function()
                if store:lock('report', 60) ~= nil then
                    locks = locks + 1
                end

                if store:add('flag', index, 60) then
                    added = added + 1
                end
            end)

            worker:set_joinable(true)
            table.insert(fibers, worker)
        end

        for _, worker in ipairs(fibers) do
            worker:join()
        end

        store:tags('docs'):put('a', 1)
        store:tags('docs'):put('b', 2)
        store:put('c', 3)

        return {
            mvcc = box.cfg.memtx_use_mvcc_engine,
            locks = locks,
            added = added,
            forgotten = store:tags('docs'):flush(),
            left = store:get('c'),
        }
    end, { helper.ON_NODE })

    t.assert_equals(report, { mvcc = true, locks = 1, added = 1, forgotten = 2, left = 3 })
end
