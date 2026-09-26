--- Общие средства проверок кэша.
---
--- Исходники читаются с диска, а не через `require`: у Tarantool свой
--- загрузчик `.rocks`, он идёт раньше `package.path` и подсунул бы
--- установленную копию пакета, если она есть. Проверки тогда шли бы
--- против вчерашнего кода, а покрытие считалось бы по нему. Зависимости
--- пакета — `tnt-must`, `tnt-clock`, `tnt-log` и `tnt-external` — берутся
--- из `.rocks` обычным `require`: проверяется этот пакет, а не они.
--- На временном узле они берутся так же.
---
--- Исключение — общий способ объявить ряд из `tnt-metrics`: его модули
--- грузятся файлами из `.rocks` вместе с пакетом, свой экземпляр
--- на каждую загрузку (ниже, `ROWS`).
---
--- Клиент `tnt-redis` и чтение окружения `tnt-env` нужны только живой
--- проверке драйвера на Redis: клиент пакету приходит аргументом,
--- а окружение называет порт стенда. Оба стоят в `.rocks` (`make deps`).
---
--- Оснастка в `test/testing/` — загрузчик исходников, часы-двойник,
--- ловушка журнала, запись файлов и временный узел — грузится так же,
--- файлами, и один раз на процесс: второй экземпляр загрузчика не знал бы,
--- что вытеснил первый, и не вернул бы вытесненное на место.
---
--- Проверки берут всё через этот помощник, а не из оснастки напрямую:
--- помощник — единственное, чем файл проверок отличается от того же файла
--- в наборе, где пакет живёт рядом со своими зависимостями.

local fiber = require('fiber')
local fio = require('fio')

--- Реестр встроенного metrics: его методов в аннотациях ядра нет.
---@type any
local registry = require('metrics')

--- Модули оснастки в порядке зависимостей: узел берёт файлы и загрузчик,
--- ловушка журнала — загрузчик.
local TESTING = {
    { name = 'tnt.testing.sources', path = 'test/testing/sources.lua' },
    { name = 'tnt.testing.files', path = 'test/testing/files.lua' },
    { name = 'tnt.testing.clock', path = 'test/testing/clock.lua' },
    { name = 'tnt.testing.journal', path = 'test/testing/journal.lua' },
    { name = 'tnt.testing.node', path = 'test/testing/node.lua' },
}

for _, module in ipairs(TESTING) do
    if package.loaded[module.name] == nil then
        local chunk, failure = loadfile(fio.abspath(module.path))

        if chunk == nil then
            error(('оснастка %s не читается: %s'):format(module.name, tostring(failure)))
        end

        package.loaded[module.name] = chunk()
    end
end

local sources = package.loaded['tnt.testing.sources']

--- Оснастка проверок под теми именами, что зовёт помощник.
local testing = {
    load_sources = sources.load,
    unload_sources = sources.unload,
    module = sources.module,
    clock = package.loaded['tnt.testing.clock'].new,
    capture_log = package.loaded['tnt.testing.journal'].capture,
    start_node = package.loaded['tnt.testing.node'].start,
    stop_node = package.loaded['tnt.testing.node'].stop,
}

local helper = {}

--- Общий способ объявить ряд — файлами из `.rocks`, свой экземпляр
--- на каждую загрузку помощника, а не `require`.
---
--- Объявленные ряды способ помнит в себе, и повторное объявление отдаёт
--- прежний ряд вместе с накопленным счётом. С одним экземпляром на процесс
--- каждая проверка видела бы счёт всех прошлых: сверка «три попадания,
--- три промаха» зависела бы от того, что прошло до неё. Свежий экземпляр
--- заводит ряд заново и снимает из реестра прежний сборщик того же ряда —
--- счёт у каждой проверки свой, с нуля.
local ROWS = {
    { name = 'tnt.metrics.series.labels', path = '.rocks/share/tarantool/tnt/metrics/series/labels.lua' },
    { name = 'tnt.metrics.series.collector', path = '.rocks/share/tarantool/tnt/metrics/series/collector.lua' },
    { name = 'tnt.metrics.series', path = '.rocks/share/tarantool/tnt/metrics/series.lua' },
}

--- Модули в порядке зависимостей: ряды, затем пакет.
helper.MODULES = {
    ROWS[1],
    ROWS[2],
    ROWS[3],
    { name = 'tnt.cache.series', path = 'tnt/cache/series.lua' },
    { name = 'tnt.cache.record', path = 'tnt/cache/record.lua' },
    { name = 'tnt.cache.lock', path = 'tnt/cache/lock.lua' },
    { name = 'tnt.cache.tagged', path = 'tnt/cache/tagged.lua' },
    { name = 'tnt.cache.store', path = 'tnt/cache/store.lua' },
    { name = 'tnt.cache.memory', path = 'tnt/cache/memory.lua' },
    { name = 'tnt.cache.redis', path = 'tnt/cache/redis.lua' },
    { name = 'tnt.cache.space', path = 'tnt/cache/space.lua' },
    { name = 'tnt.cache', path = 'tnt/cache.lua' },
}

--- Модули пакета с путями от корня — узлу, который собирает кэш заново
--- перед каждой проверкой: узел живёт в своём каталоге.
helper.ON_NODE = sources.absolute(helper.MODULES)

--- Загрузчик исходников оснастки на узле: функцию в `server:exec`
--- не передать, туда уходит только её тело, и кэш узел собирает им сам.
local LOADER = { { name = 'tnt.testing.sources', path = 'test/testing/sources.lua' } }

--- Поднимает узел с настоящим box и загрузчиком исходников.
---@param box_cfg table|nil Настройки `box.cfg` поверх умолчаний оснастки
---@return table server
function helper.start_node(box_cfg)
    return testing.start_node({ modules = LOADER, box_cfg = box_cfg })
end

--- Останавливает узел и убирает его каталог.
helper.stop_node = testing.stop_node

--- Клиент Redis для живых проверок — `tnt-redis` из `.rocks`: пакету он
--- приходит аргументом, а проверкам нужен настоящий.
---@return table redis Фасад `tnt.redis`
function helper.redis()
    return require('tnt.redis')
end

--- Отвечает ли кто-нибудь на порту стенда.
---@param host string
---@param port integer
---@return boolean
function helper.listening(host, port)
    local connection = require('socket').tcp_connect(host, port, 0.3)

    if connection == nil then
        return false
    end

    connection:close()

    return true
end

--- Чтение окружения для настроек живых проверок: порт стенда.
---
--- `tnt-env` приходит из `.rocks`: сам пакет окружения не читает,
--- и его зависимостью он не объявлен — его ставит `make deps`.
---@return table
function helper.stand_env()
    return require('tnt.env')
end

--- Загружает исходники заново.
---@return any cache
function helper.load()
    return testing.load_sources(helper.MODULES, 'tnt.cache')
end

--- Уже загруженный модуль пакета — часть той же загрузки, что фасад.
helper.module = testing.module

--- Убирает исходники: следующая проверка грузит их заново.
function helper.unload()
    testing.unload_sources(helper.MODULES)
end

--- Совпадают ли метки: одинаковый набор имён с одинаковыми значениями.
---@param left table
---@param right table
---@return boolean
local function same_labels(left, right)
    for name, value in pairs(left) do
        if right[name] ~= value then
            return false
        end
    end

    for name in pairs(right) do
        if left[name] == nil then
            return false
        end
    end

    return true
end

--- Что увидит сборщик: число ряда с ровно такими метками.
---
--- Реестр читается так же, как его читает выкладка, — после обработчиков
--- сбора. Исходники рядов грузятся заново каждой проверкой вместе
--- с пакетом, и счёт у каждой проверки свой, с нуля.
---@param name string
---@param labels table|nil
---@return number|nil
function helper.value(name, labels)
    for _, observation in ipairs(registry.collect({ invoke_callbacks = true })) do
        if observation.metric_name == name and same_labels(observation.label_pairs, labels or {}) then
            return observation.value
        end
    end

    return nil
end

--- Часы, которые двигает только проверка: двойник из оснастки, кэшу
--- нужны его стенные часы (`realtime`) и перевод вперёд (`advance`).
helper.clock = testing.clock

--- Ловушка журнала: промах по отказу драйвера виден только записью,
--- и без ловушки она ушла бы в вывод самого прогона. Взводится заново
--- каждой проверкой — `forget`.
helper.journal = testing.capture_log()

--- Свежие исходники и ловушка журнала — перед каждой проверкой.
---
--- Ловушка взводится заново: она одна на процесс, и записи соседа
--- иначе достались бы этой проверке.
---@return any cache
function helper.fresh()
    helper.journal.forget()

    return helper.load()
end

--- Хранилище с проверочными часами; по умолчанию — в памяти.
---@param cache any Загруженный пакет
---@param clock table Часы-двойник
---@param opts table|nil
---@return any
function helper.store(cache, clock, opts)
    opts = opts or {}
    opts.now = clock.realtime

    return cache.new(opts)
end

--- Десять промахов разом на одном ключе: `remember` с замком из десяти
--- файберов, а счёт спит 0,1 с — остальные успевают прийти за ним.
---@param store any Хранилище
---@param key string
---@return integer calls Сколько раз считали
---@return any[] got Что получил каждый файбер
function helper.stampede(store, key)
    local calls = 0
    local got = {}
    local fibers = {}

    for index = 1, 10 do
        local worker = fiber.new(function()
            got[index] = store:remember(key, 60, function()
                calls = calls + 1
                fiber.sleep(0.1)

                return 'страница'
            end, { lock = 5 })
        end)

        worker:set_joinable(true)
        table.insert(fibers, worker)
    end

    for _, worker in ipairs(fibers) do
        worker:join()
    end

    return calls, got
end

--- Имена полей записи кэша по номерам: двойник отдаёт запись и по имени,
--- и по номеру, как кортеж.
local FIELDS = { key = 1, value = 2, expires_at = 3, tags = 4 }

--- Запись двойника: поля по номерам, имена — через метатаблицу.
local Row = {
    __index = function(row, name)
        local at = FIELDS[name]

        return at and rawget(row, at)
    end,
}

--- Бросает отказ box «такой ключ уже есть», как настоящий спейс.
local function duplicate()
    ---@type any
    local errors = box.error

    error(errors.new({ code = errors.TUPLE_FOUND, reason = 'Duplicate key exists' }))
end

--- Двойник спейса кэша: get, replace, insert, delete, truncate, select —
--- как у box, с первичным индексом TREE по ключу.
---
--- `select` знает ровно то, что нужно обходу: `GE` от пустого ключа —
--- с начала, `GT` от ключа — строго после него, `limit` — не больше.
--- Порядок — как у TREE по строке: по байтам. `insert` занятого ключа
--- бросает отказ box с кодом `TUPLE_FOUND`, `delete` отдаёт стёртое.
--- Имени у двойника нет: по имени фасад искал бы спейс тегов в box.
---@return table space
function helper.fake_space()
    local rows = {}

    return {
        index = { [0] = { type = 'TREE' } },
        get = function(_, key)
            return rows[key]
        end,
        replace = function(_, tuple)
            rows[tuple[1]] = setmetatable({ tuple[1], tuple[2], tuple[3], tuple[4] }, Row)
        end,
        insert = function(self, tuple)
            if rows[tuple[1]] ~= nil then
                duplicate()
            end

            self:replace(tuple)
        end,
        delete = function(_, key)
            local old = rows[key]

            rows[key] = nil

            return old
        end,
        truncate = function()
            rows = {}
        end,
        select = function(_, key, opts)
            local after = opts.iterator == 'GT' and key[1] or nil
            local keys = {}

            for stored in pairs(rows) do
                if after == nil or stored > after then
                    table.insert(keys, stored)
                end
            end

            table.sort(keys)

            local found = {}

            for index = 1, math.min(opts.limit, #keys) do
                table.insert(found, rows[keys[index]])
            end

            return found
        end,
    }
end

--- Двойник спейса тегов: строки «тег, ключ» с первичным ключом по обоим
--- полям; `select` — `EQ` по тегу, ключи по порядку, не больше `limit`.
---@return table tags
---@return table<string, table<string, boolean>> rows Тег → ключи — для проверок
function helper.fake_tags()
    local rows = {}

    local tags = {
        replace = function(_, row)
            rows[row[1]] = rows[row[1]] or {}
            rows[row[1]][row[2]] = true
        end,
        delete = function(_, key)
            local keys = rows[key[1]] or {}

            keys[key[2]] = nil

            if next(keys) == nil then
                rows[key[1]] = nil
            end
        end,
        truncate = function()
            for tag in pairs(rows) do
                rows[tag] = nil
            end
        end,
        select = function(_, key, opts)
            assert(opts.iterator == 'EQ', 'строки тега выбирают по равенству')

            local keys = {}

            for stored in pairs(rows[key[1]] or {}) do
                table.insert(keys, stored)
            end

            table.sort(keys)

            local found = {}

            for index = 1, math.min(opts.limit, #keys) do
                table.insert(found, { key[1], keys[index] })
            end

            return found
        end,
    }

    return tags, rows
end

--- Спейсы по имени для драйвера `named`: проверка заводит их в таблице
--- сама, как шаг миграции, а внешняя зависимость пишет, о каких именах
--- драйвер спрашивал. Транзакция — просто вызов.
---@param cache any Загруженный пакет
---@return table<string, table> spaces Спейсы по имени — их заполняет проверка
---@return fun(): string[] asked Имена, о которых спрашивали с прошлого вызова
function helper.named_spaces(cache)
    local spaces, asked = {}, {}

    cache.space._set_source({
        atomic = function(fn, ...)
            return fn(...)
        end,
        space = function(name)
            table.insert(asked, name)

            return spaces[name]
        end,
    })

    return spaces, function()
        local names = asked

        asked = {}

        return names
    end
end

--- Драйвер-двойник поверх данного: считает вызовы `scan` и `erase_many`,
--- чтобы проверка видела куски и партии, а не только итог.
---@param inner table Драйвер, который делает работу
---@return table driver
---@return { scans: integer[], erased: string[][] } calls Пределы кусков и стёртые партии
function helper.counting_driver(inner)
    local calls = { scans = {}, erased = {} }

    local driver = setmetatable({
        scan = function(cursor, limit)
            table.insert(calls.scans, limit)

            return inner.scan(cursor, limit)
        end,
        erase_many = function(keys)
            table.insert(calls.erased, keys)

            return inner.erase_many(keys)
        end,
    }, { __index = inner })

    return driver, calls
end

--- Клиент tnt-redis двойником: пишет каждую команду с настройками
--- вызова и отвечает по сценарию — по ответу на команду, по порядку.
---
--- Ответ — список `{ значение, отказ }`: `{ 'OK' }`, `{}` — промах,
--- `{ nil, err }` — отказ клиента. Команда сверх сценария — бросок:
--- драйвер, пославший лишнее, обязан упасть в проверке, а не получить
--- пустоту за ответ.
---@param replies table[] Ответы по порядку
---@return table client У клиента поле `calls` — посланное: `{ args, opts }`
function helper.fake_redis(replies)
    local client = { calls = {} }

    function client:command(args, opts)
        table.insert(self.calls, { args = args, opts = opts })

        local reply = table.remove(replies, 1)

        if reply == nil then
            error(('двойнику Redis ответить на %s нечем'):format(args[1]), 0)
        end

        return reply[1], reply[2]
    end

    return client
end

return helper
