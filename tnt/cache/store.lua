--- Хранилище кэша: срок жизни, приставка ключей и договор над драйвером.
---
--- Драйвер знает только, как прочитать запись с её сроком, записать,
--- стереть — по одному и партией, — очистить и отдать кусок обхода;
--- всё остальное (срок вышел или нет, «запомнить, если нет», счётчик,
--- замок, уступки и бюджет обхода) считается хранилищем и его частями
--- один раз на всех.
---
--- **Что пишет по прочитанному, драйвер вправе сделать сам одним
--- действием** — `add`, `take` (прочитать и стереть — для `pull`),
--- `increment` и `erase_if` (стереть, если под ключом это значение, —
--- для снятия замка). Драйверу в памяти это не нужно: между чтением
--- и записью у него нет уступки. У драйвера по сети она есть, и соседний
--- узел успел бы вклиниться между двумя командами — замок `add` достался
--- бы двоим, счётчик потерял бы прибавку. Нет действия — хранилище
--- собирает его из чтения и записи. Драйвер на спейсе даёт `insert` —
--- положить, только если ключа нет вовсе: чтение с `memtx_use_mvcc_engine`
--- не видит чужой вставки, пока та пишется в WAL, и «положить, если нет»
--- из чтения и замены отдало бы замок двоим; хранилище тогда стирает
--- просроченное чтением, а кладёт вставкой.
---
--- **Драйвер по сети (`remote`) внутри транзакции box не зовётся** —
--- исключение: уступка на сети порвала бы транзакцию memtx, и вызывающий
--- узнал бы об этом только на фиксации, а записанное в кэш при откате
--- осталось бы лежать.
---
--- Срок — секунды; пусто — навсегда. Просроченная запись читается как
--- отсутствующая и тут же стирается: драйверу не нужен свой сторож,
--- а память под мёртвые записи держится не дольше, чем до первого
--- обращения к ним. Стирает по дороге только пишущий узел: на узле
--- только для чтения (`box.info.ro`) просроченное читается как
--- отсутствующее и остаётся лежать. Иначе чтение на реплике падало бы
--- ровно тогда, когда у записи вышел срок, — стирание там отказывает,
--- как всякая запись в спейс. Стереть его там некому, кроме ведущего
--- и `sweep`; кто хочет чистить заранее, зовёт `sweep` сам.
---
--- **Вид отказа назван у каждого действия.** Чтение — `get`, `has`,
--- поиск в `remember` и `pull` — отказ драйвера считает промахом и
--- пишет о нём в журнал `tnt.cache` уровнем warn: вызывающему промах
--- безопасен, он пересчитает значение, а молчать о неотвечающем кэше
--- нельзя — иначе о нём узнают по нагрузке на хранилище. Запись —
--- `put`, `forget`, `flush`, забывание тега, снятие замка — бросает:
--- потерянную запись не заметит никто, а тег, забытый не до конца,
--- оставил бы устаревшее значение. Бросает и то, что решает
--- по прочитанному, — `add`, `increment`, `decrement`, взятие замка:
--- промах на условии записи означал бы «пиши поверх чужого» и «считай
--- чужой счётчик с нуля». `sweep` отдаёт отказ box парой: стёртое
--- до отказа остаётся стёртым. Занятый замок и ожидание, кончившееся
--- ни с чем, — пара `nil, err`: так бывает, и решает вызывающий.
--- Ошибка программиста — негодный ключ, пустое значение, негодный срок,
--- шаг прибавки, теги и настройки замка, вызов `sweep`, забывания тега
--- и ожидания замка либо драйвера по сети внутри транзакции — бросает
--- всегда и отказом не считается.
---
--- **Обращения за значением видны рядами метрик** (`tnt.cache.series`):
--- сколько их было по хранилищу и итогу — попадание, промах, отказ — и
--- сколько шло чтение драйвером. Хранилище в метке — по имени `name`,
--- по умолчанию это имя драйвера.
---
--- **`sweep` идёт кусками.** Драйвер отдаёт ключи со сроками партиями
--- по `chunk` от курсора продолжения, а не одним итератором на весь
--- обход: перед каждым куском хранилище уступает управление, и обход
--- спейса на сотни тысяч записей не упирается в срез файбера (ядро рвёт
--- обход без уступки через секунду — «fiber slice is exceeded», проверено
--- на 3.8: полный `pairs` при срезе в 20 мс падает, куски по `GT`
--- с `limit` и уступкой проходят, а куски без уступки падают так же).
--- Просроченное из куска стирается партией — у спейса одной транзакцией
--- вместо своей на каждый ключ. `budget` ограничивает просмотренное
--- за вызов: кончился — отчёт отдаёт `cursor`, и следующий вызов
--- продолжает с него. Отказ box посреди обхода — реплика, снесённый
--- спейс — пара `nil, err`; уступка обрывает транзакцию memtx, поэтому
--- вызов внутри транзакции бросает. Обход — не снимок: что видно
--- из дописанного и стёртого соседями за уступкой, называет каждый
--- драйвер.
---
---     local report, err = store:sweep()                  -- до конца
---     local report, err = store:sweep({ budget = 5000 }) -- не дольше 5000 ключей
---
---     while report ~= nil and report.cursor ~= nil do
---         report, err = store:sweep({ budget = 5000, cursor = report.cursor })
---     end
---
--- **Части хранилища — по модулю на дело.** Запись через драйвер — ключ,
--- срок, живое значение, «положить, если нет» — лежит
--- в `tnt.cache.record`; замок на ключ и `remember` с замком —
--- в `tnt.cache.lock`; вид с тегами — в `tnt.cache.tagged`. Здесь —
--- чтение, запись, счётчик, обход и тонкие входы `lock`, `remember`
--- и `tags`: хранилище отдаёт себя частям аргументом. Так правка замка
--- или тегов не задевает обход и счётчик, а договор над драйвером
--- остаётся одним на всех.

local fiber = require('fiber')
local clock = require('tnt.clock')
local lock = require('tnt.cache.lock')
local record = require('tnt.cache.record')
local tagged = require('tnt.cache.tagged')

--- Бросок без места в коде: у отказа настройки места нет, у чужой
--- поломки, брошенной заново, оно уже есть. Значение идёт как есть —
--- чужая поломка остаётся тем, чем её бросили.
local fail = require('tnt.must.fail').raise

---@class TntCacheEntry Ключ со сроком — то, что драйвер отдаёт обходу
---@field key string
---@field expires_at number

---@class TntCacheDriver Срок 0 — навсегда либо срок ведёт сам драйвер, как redis
---@field read fun(key: string): any, number|nil, string[]|nil Значение, срок (секунды эпохи), теги
---@field write fun(key: string, value: any, expires_at: number, tags: string[]|nil) Теги без повторов
---@field erase fun(key: string)
---@field erase_many fun(keys: string[]) Стирает партию — у спейса одной транзакцией
---@field clear fun()
---@field scan fun(cursor: any, limit: integer): TntCacheEntry[], any Кусок после курсора и курсор дальше; пусто — конец
---@field name string Имя драйвера — для status
---@field add (fun(key: string, value: any, expires_at: number, tags: string[]|nil): boolean)|nil Если живого нет
---@field insert (fun(key: string, value: any, expires_at: number, tags: string[]|nil): boolean)|nil Если ключа нет
---@field take (fun(key: string): any, number|nil)|nil Прочитать и стереть разом — ответ как у `read`
---@field increment (fun(key: string, by: number, expires_at: number): number|nil)|nil Прибавить разом; пусто — не число
---@field erase_if (fun(key: string, value: any): boolean)|nil Стереть, если под ключом это значение, — разом
---@field erase_tagged (fun(tag: string, limit: integer): integer, integer)|nil Кусок тега: взято и стёрто
---@field tagless string|nil Почему тегов нет — для отказа
---@field remote boolean|nil Ходит по сети: внутри транзакции box не зовётся

---@class TntCacheSweepOptions
---@field chunk integer|nil Сколько ключей брать у драйвера за раз; по умолчанию 1000
---@field budget integer|nil Сколько ключей просмотреть за вызов; пусто — до конца
---@field cursor any Откуда продолжить — из отчёта прошлого вызова

---@class TntCacheSweepReport
---@field swept integer Сколько стёрто за вызов
---@field scanned integer Сколько просмотрено за вызов
---@field cursor any Откуда продолжить, если кончился бюджет

---@class TntCacheStore
---@field name string Имя хранилища в рядах метрик
---@field driver TntCacheDriver
---@field prefix string
---@field ttl number|nil Срок по умолчанию, секунды
---@field now fun(): number
local Store = {}
Store.__index = Store

--- Срок «навсегда» в записи и шаг опроса замка — наружу: драйвер redis
--- сверяет с «навсегда» срок, который ему дали, а документ зовёт шаг
--- опроса по имени хранилища.
local Module = { FOREVER = record.FOREVER, POLL = lock.POLL }

--- Подмена внешних средств — одна на хранилище, замок и вид с тегами;
--- сами средства держит `tnt.cache.record`. Только для проверок.
Module._set_source = record._set_source

--- Внешние средства — общие с замком и видом с тегами.
local source = record.source

--- Тексты отказов обхода.
local INSIDE_TRANSACTION =
    'кэш: sweep уступает управление и сам фиксирует партии — внутри транзакции его не зовут'
local STOPPED =
    'кэш: чистка не дошла до конца — стёрто %d, дальше box отказал: %s'

--- Отказ хранилищу без имени.
local BAD_NAME =
    'кэш: имя хранилища — непустая строка: name в настройках либо name драйвера'

--- Значение по ключу либо умолчание. Отказ драйвера — промах.
---@param key string
---@param default any
---@return any
function Store:get(key, default)
    local value, found = record.asked(self, record.keyed(self, key), self.driver.read)

    if not found then
        return default
    end

    return value
end

--- Есть ли живое значение. Отказ драйвера — промах, то есть `false`.
---@param key string
---@return boolean
function Store:has(key)
    return select(2, record.asked(self, record.keyed(self, key), self.driver.read)) == true
end

--- Кладёт значение на срок; пусто — навсегда либо срок по умолчанию.
--- Отказ хранилища бросает: запись не теряют молча.
---@param key string
---@param value any
---@param ttl number|nil
function Store:put(key, value, ttl)
    record.stored(self, key, value, ttl, nil)
end

--- Кладёт, только если живого значения нет.
---@param key string
---@param value any
---@param ttl number|nil
---@return boolean added
function Store:add(key, value, ttl)
    return record.placed(self, key, value, ttl, nil)
end

--- Берёт замок на ключ (`tnt.cache.lock`).
---
--- Занят и за `wait` не освободился — `nil, err`: так бывает, и решать,
--- ждать ли дольше, вызывающему. Отказ драйвера бросает, как у `add`:
--- промах здесь означал бы «замок свободен».
---@param key string
---@param ttl number Срок замка, секунды
---@param opts TntCacheLockOptions|nil
---@return TntCacheLock|nil lock
---@return string|nil err
function Store:lock(key, ttl, opts)
    return lock.take(self, key, ttl, opts)
end

--- Значение по ключу, а если его нет — вычисленное и сохранённое.
---
--- Отказ драйвера на чтении — промах: значение считается заново, и это
--- ровно то, ради чего кэш и заводят. Сохранение после этого бросает,
--- как всякая запись.
---
--- С настройками `{ lock, wait }` значение считает один: промах берёт
--- замок ключа на `lock` секунд, прочие ждут значения не дольше `wait`
--- (по умолчанию — срока замка). Не дождались — `nil, err`. Настройки
--- проверяются и при попадании: ошибка программиста бросает всегда,
--- а не на первом промахе.
---@param key string
---@param ttl number|nil
---@param compute fun(): any
---@param opts TntCacheRememberOptions|nil
---@return any value
---@return string|nil err
function Store:remember(key, ttl, compute, opts)
    return lock.remember(self, key, ttl, compute, nil, opts)
end

--- Значение по ключу с удалением.
---
--- Стирается только прочитанное: промах — в том числе промах по отказу
--- драйвера — ничего не забрал, и стирать ему нечего. Иначе отказ
--- чтения уносил бы непрочитанное значение молча, а `pull` по пустому
--- ключу писал бы в спейс на ровном месте. Драйвер с `take` читает
--- и стирает одним действием: одноразовый знак, забранный двумя узлами
--- разом, достался бы обоим.
---@param key string
---@param default any
---@return any
function Store:pull(key, default)
    local full = record.keyed(self, key)
    local take = self.driver.take
    local value, found = record.asked(self, full, take or self.driver.read)

    if not found then
        return default
    end

    if take == nil then
        self.driver.erase(full)
    end

    return value
end

--- Стирает значение. Отказ хранилища бросает: запись не теряют молча.
---@param key string
function Store:forget(key)
    self.driver.erase(record.keyed(self, key))
end

--- Шаг прибавки: пусто — единица, иначе число.
---@param by any
---@return number
local function step_of(by)
    if by == nil then
        return 1
    end

    if type(by) ~= 'number' then
        fail('кэш: шаг прибавки должен быть числом')
    end

    return by
end

--- Прибавка из чтения и записи — для драйвера без своей прибавки.
---
--- Чтение здесь — основание записи, и отказ драйвера бросает: промах
--- начал бы чужой счётчик с нуля и стёр бы накопленное. Теги живой
--- записи пишутся обратно вместе со сроком: прибавка меняет значение,
--- а не то, чем запись помечена.
---@param self TntCacheStore
---@param full string Ключ с приставкой
---@param step number
---@return number|nil updated Пусто — там не число
local function added(self, full, step)
    local value, found, expires_at, tags = record.alive(self, full, self.driver.read)
    local current = 0

    if found then
        current = value
    end

    if type(current) ~= 'number' then
        return nil
    end

    local updated = current + step

    self.driver.write(full, updated, expires_at or record.expiry(self, nil), tags)

    return updated
end

--- Прибавляет к числу; отсутствующее считается нулём, со сроком
--- по умолчанию. Срок живой записи сохраняется.
---@param self TntCacheStore
---@param key string
---@param step number
---@return number
local function counted(self, key, step)
    local full = record.keyed(self, key)
    local increment = self.driver.increment
    local updated

    if increment ~= nil then
        updated = increment(full, step, record.expiry(self, nil))
    else
        updated = added(self, full, step)
    end

    if updated == nil then
        fail(('кэш: %s — не число, прибавлять нечего'):format(key))
    end

    return updated
end

--- Прибавляет к числу; отсутствующее считается нулём. Срок сохраняется.
---@param key string
---@param by number|nil
---@return number
function Store:increment(key, by)
    return counted(self, key, step_of(by))
end

--- Убавляет число.
---@param key string
---@param by number|nil
---@return number
function Store:decrement(key, by)
    return counted(self, key, -step_of(by))
end

--- Стирает всё — вместе с чужими приставками, тегами и замками: драйвер
--- один на всех. Отказ хранилища бросает, как и всякая запись.
function Store:flush()
    record.outside_transaction(self)
    self.driver.clear()
end

--- Вид хранилища, помечающий записи тегами (`tnt.cache.tagged`).
---
--- Драйвер без `erase_tagged` тегов не знает, и это ошибка программиста
--- уже здесь, а не на забывании: тег, положенный туда, где его потом
--- не забыть, оставил бы устаревшее значение жить до срока.
---@param names string|string[] Тег либо список тегов
---@return TntCacheTagged
function Store:tags(names)
    return tagged.new(self, names)
end

--- Число ключей в настройке обхода: целое больше нуля либо пусто.
---@param value any
---@param name string
local function count_option(value, name)
    if value ~= nil and (type(value) ~= 'number' or value < 1 or value % 1 ~= 0) then
        fail(('кэш: %s должен быть целым числом больше нуля'):format(name))
    end
end

--- Ключи куска, чей срок вышел к этому мигу.
---@param entries TntCacheEntry[]
---@param now number
---@return string[]
local function expired_of(entries, now)
    local dead = {}

    for _, entry in ipairs(entries) do
        if entry.expires_at ~= record.FOREVER and entry.expires_at <= now then
            table.insert(dead, entry.key)
        end
    end

    return dead
end

--- Обходит записи кусками и стирает просроченное.
---
--- Исключения отсюда летят как есть: `sweep` ловит их на одной границе
--- и отказом делает только те, что бросил box.
---@param self TntCacheStore
---@param opts TntCacheSweepOptions
---@param report TntCacheSweepReport Заполняется по ходу: при отказе видно, сколько успели
local function walk(self, opts, report)
    local chunk = opts.chunk or record.CHUNK
    local budget = opts.budget
    local cursor = opts.cursor

    repeat
        source().yield()

        -- Часы — на каждый кусок: за уступкой время идёт, и запись, чей
        -- срок вышел, пока обход стоял, стирается этим же обходом.
        local now = self.now()
        local want = chunk

        if budget ~= nil then
            want = math.min(chunk, budget - report.scanned)
        end

        local entries
        entries, cursor = self.driver.scan(cursor, want)

        local dead = expired_of(entries, now)

        if #dead > 0 then
            self.driver.erase_many(dead)
        end

        report.scanned = report.scanned + #entries
        report.swept = report.swept + #dead

        -- Курсор значит «бюджет кончился», а не «ключи остались»: обход,
        -- кончившийся ровно на бюджете, вправе отдать курсор — следующий
        -- вызов с ним вернёт пустой отчёт без курсора.
        if report.scanned == budget then
            report.cursor = cursor

            return
        end
    until cursor == nil
end

--- Стирает просроченные записи заранее — кусками, с уступкой перед каждым.
---
--- Отказ `nil, err` — box отказал посреди обхода: реплика, снесённый спейс.
--- Стёртое до отказа остаётся стёртым: обход — не транзакция. Негодные
--- настройки, вызов внутри транзакции и поломка драйвера бросают; отмена
--- файбера тоже — это приказ остановиться, а не сбой чистки.
---@param opts TntCacheSweepOptions|nil
---@return TntCacheSweepReport|nil report
---@return string|nil err
function Store:sweep(opts)
    opts = opts or {}

    if type(opts) ~= 'table' then
        fail('кэш: настройки sweep должны быть таблицей')
    end

    count_option(opts.chunk, 'chunk')
    count_option(opts.budget, 'budget')

    if source().in_transaction() then
        fail(INSIDE_TRANSACTION)
    end

    ---@type TntCacheSweepReport
    local report = { swept = 0, scanned = 0 }

    local ok, failure = pcall(walk, self, opts, report)

    if ok then
        return report
    end

    -- Отмена файбера, спрятанная в отказ, оставила бы отменённый файбер
    -- жить дальше.
    fiber.testcancel()

    -- Отказом становится только то, что бросил box: всё прочее — поломка
    -- драйвера или самого обхода, и пара её спрятала бы. `box.error.is`
    -- в аннотациях ядра не описан.
    ---@diagnostic disable-next-line: undefined-field
    if box.error.is(failure) then
        return nil, STOPPED:format(report.swept, tostring(failure))
    end

    fail(failure)
end

--- Что настроено; значений здесь нет.
---@return table
function Store:status()
    return { name = self.name, driver = self.driver.name, prefix = self.prefix, ttl = self.ttl }
end

---@class TntCacheStoreOptions
---@field driver TntCacheDriver
---@field name string|nil Имя хранилища в рядах метрик; по умолчанию — имя драйвера
---@field prefix string|nil Приставка ключей; по умолчанию пусто
---@field ttl number|nil Срок по умолчанию, секунды; пусто — навсегда
---@field now (fun(): number)|nil Часы; по умолчанию tnt.clock.realtime

--- Заводит хранилище над драйвером.
---@param opts TntCacheStoreOptions
---@return TntCacheStore
local function new(opts)
    if opts.prefix ~= nil and type(opts.prefix) ~= 'string' then
        fail('кэш: prefix должна быть строкой')
    end

    if opts.ttl ~= nil and (type(opts.ttl) ~= 'number' or opts.ttl <= 0) then
        fail('кэш: ttl должен быть числом секунд больше нуля')
    end

    -- Имя — метка рядов метрик. Без него хранилище упало бы на первом
    -- чтении, у метки без значения, — далеко от строки, где его заводили.
    local name = opts.name

    if name == nil then
        name = opts.driver.name
    end

    if type(name) ~= 'string' or name == '' then
        fail(BAD_NAME)
    end

    return setmetatable({
        name = name,
        driver = opts.driver,
        prefix = opts.prefix or '',
        ttl = opts.ttl,
        now = opts.now or clock.realtime,
    }, Store)
end

Module.new = new

return Module
