--- Запись кэша через драйвер: ключ, срок, живое значение и «положить,
--- если нет».
---
--- Здесь то, что читает и пишет одну запись: проверка ключа и приставка,
--- срок по `ttl`, чтение живого со стиранием просроченного, промах
--- по отказу драйвера и «положить, если нет», собранное из чтения
--- и записи. Хранилище (`tnt.cache.store`), замок (`tnt.cache.lock`)
--- и вид с тегами (`tnt.cache.tagged`) берут это отсюда: у каждого свой
--- модуль, а договор над драйвером один, и разойтись ему негде. Этот
--- модуль не зовёт ни одного из трёх, и цикла `require` нет: хранилище
--- отдаёт себя замку и виду аргументом, а всё, что им нужно от записи,
--- лежит здесь.
---
--- Здесь же внешние зависимости всех четырёх: одна подмена в проверке
--- доходит до каждого, и проверке не нужно знать, в каком модуле живёт
--- уступка, часы ожидания или метка держателя. Подменяют их через
--- `tnt.cache.store._set_source`.
---
--- И здесь же обращение за значением ложится в ряды метрик
--- (`tnt.cache.series`): читают через драйвер только отсюда, и итог
--- чтения — попадание, промах, отказ — известен только здесь.

local fiber = require('fiber')
local uuid = require('uuid')
local clock = require('tnt.clock')
local external = require('tnt.external')
local series = require('tnt.cache.series')

--- Бросок без места в коде: у отказа настройки места нет.
local fail = require('tnt.must.fail').raise

--- Журнал промахов: подряд идущие повторы подавляются.
---
--- Драйвер, потерявший связь, отказывает на каждом чтении, и запись
--- на каждый промах утопила бы журнал сама. `changes` пишет первую
--- запись серии, а сколько их было, кладёт в ту, что серию прервала.
--- Ключа в записи нет нарочно: он у каждого промаха свой, серия с ним
--- не сложилась бы никогда, а вид отказа один на всех.
local log = require('tnt.log').changes('tnt.cache')

--- Срок «навсегда» в записи.
local FOREVER = 0

--- Сколько ключей брать у драйвера за раз — кусок обхода `sweep`
--- и забывания тега.
---
--- Тысяча стираний — одна запись в WAL и единицы миллисекунд без уступки.
local CHUNK = 1000

--- Запись о промахе по отказу драйвера.
local MISSED = 'кэш: драйвер {driver} отказал на чтении — промах'

--- Драйвер по сети внутри транзакции.
local REMOTE_IN_TRANSACTION = 'кэш: драйвер %s ходит по сети — внутри транзакции box его не зовут: '
    .. 'уступка порвёт транзакцию, а записанное в него при откате останется'

---@class TntCacheRecord
---@field _set_source fun(replacement: table|nil) Подмена средств — для проверок; ставит её `external.install`
local Module = { FOREVER = FOREVER, CHUNK = CHUNK }

--- Внешние средства: уступка, пауза, часы ожидания, метка держателя,
--- признак транзакции и признак узла только для чтения.
---
--- Через внешнюю зависимость затем, чтобы проверки вставали на границу куска и делали там
--- то, что делают соседние файберы: дописывали, стирали, двигали часы.
--- Признак транзакции спрашивается у box только на поднятом инстансе:
--- до `box.cfg` поле `is_in_txn` бросает, а драйверу в памяти box не нужен.
--- Признак «только для чтения» — тоже: у неподнятого инстанса `box.info.ro`
--- равен true, а запрещать стирание в памяти процесса ему нечем.
local source = external.install(Module, {
    yield = fiber.yield,
    sleep = clock.sleep,
    monotonic = clock.monotonic,
    scheduler_now = clock.scheduler_now,

    token = function()
        return uuid.str()
    end,

    in_transaction = function()
        return type(box.cfg) == 'table' and box.is_in_txn()
    end,

    read_only = function()
        return type(box.cfg) == 'table' and box.info.ro == true
    end,
})

--- Действующие внешние средства — хранилищу, замку и виду с тегами.
Module.source = source

--- Драйвер по сети внутри транзакции box — ошибка программиста.
---
--- Проверяется до обращения к драйверу: у чтения бросок драйвера стал бы
--- промахом, и ошибка программиста спряталась бы в журнал.
---@param store TntCacheStore
local function outside_transaction(store)
    if store.driver.remote and source().in_transaction() then
        fail(REMOTE_IN_TRANSACTION:format(store.driver.name))
    end
end

--- Ключ, проверенный до обращения к драйверу: непустая строка, а драйвер
--- по сети — вне транзакции. Ключ спрашивает каждое действие с ключом.
---@param store TntCacheStore
---@param key any
---@return string
local function checked(store, key)
    if type(key) ~= 'string' or key == '' then
        fail('кэш: ключ должен быть непустой строкой')
    end

    outside_transaction(store)

    return key
end

--- Ключ значения с приставкой.
---@param store TntCacheStore
---@param key any
---@return string
local function keyed(store, key)
    return store.prefix .. checked(store, key)
end

--- Пустое значение положить нельзя — это ошибка программиста.
---@param value any
local function present(value)
    if value == nil then
        fail('кэш: значение не может быть пустым, для удаления есть forget')
    end
end

--- Срок записи по данному ttl либо умолчанию.
---@param store TntCacheStore
---@param ttl any
---@return number expires_at
local function expiry(store, ttl)
    local seconds = ttl

    if seconds == nil then
        seconds = store.ttl
    end

    if seconds == nil then
        return FOREVER
    end

    if type(seconds) ~= 'number' or seconds <= 0 then
        fail('кэш: срок должен быть числом секунд больше нуля')
    end

    return store.now() + seconds
end

--- Живое значение по ключу; просроченное стирается по дороге. Отказ
--- драйвера бросает: так читает тот, кто по прочитанному пишет.
---@param store TntCacheStore
---@param full string Ключ с приставкой
---@param read fun(key: string): any, number|nil, string[]|nil Чем читать: `read` драйвера либо его `take`
---@return any value
---@return boolean|nil found Пусто — нет
---@return number|nil expires_at Срок живого
---@return string[]|nil tags Теги живого
local function alive(store, full, read)
    local value, expires_at, tags = read(full)

    if expires_at == nil then
        return nil
    end

    if expires_at ~= FOREVER and expires_at <= store.now() then
        -- На узле только для чтения стирать нечем: `erase` отказал бы,
        -- как всякая запись, и чтение падало бы ровно тогда, когда у
        -- записи вышел срок. Промах вызывающему честен и без стирания,
        -- а стереть остаётся ведущему и `sweep`.
        if not source().read_only() then
            store.driver.erase(full)
        end

        return nil
    end

    return value, true, expires_at, tags
end

--- Живое значение по ключу, чем бы чтение ни кончилось.
---
--- Отказ драйвера здесь промах, а не бросок: кэш — не источник правды,
--- и вызывающему безопаснее пересчитать значение, чем получить в лицо
--- поломку сети. Прячется любой бросок драйвера, а не только отказ box:
--- у redis-драйвера отказ сети — обычное дело, и отличить его от чужой
--- опечатки хранилищу нечем. След остаётся один — запись в журнале.
---@param store TntCacheStore
---@param full string Ключ с приставкой
---@param read fun(key: string): any, number|nil Чем читать
---@return any value
---@return boolean|nil found Пусто — нет
---@return boolean|nil failed Промах по отказу драйвера
local function fetched(store, full, read)
    local ok, value, found = pcall(alive, store, full, read)

    if ok then
        return value, found
    end

    -- Отмена файбера, спрятанная в промах, оставила бы отменённый файбер
    -- жить дальше.
    fiber.testcancel()

    -- Причину pcall кладёт на место первого возвращённого значения.
    log.warn(MISSED, { driver = store.driver.name, err = tostring(value) })

    return nil, nil, true
end

--- Обращение к кэшу за значением: живое значение по ключу, итог которого
--- и длительность чтения ложатся в ряды метрик.
---
--- Так читает то, с чего начинается обращение вызывающего: `get`, `has`,
--- `pull` и первое чтение `remember`. Повторное чтение под замком и опрос
--- ожидающих идут через `fetched` мимо рядов — они часть того же
--- обращения. Часы — монотонные внешние: перевод стенных длительность
--- не сбивает, а проверки подменяют их вместе с остальными.
---@param store TntCacheStore
---@param full string Ключ с приставкой
---@param read fun(key: string): any, number|nil Чем читать
---@return any value
---@return boolean|nil found Пусто — нет
local function asked(store, full, read)
    local started = source().monotonic()
    local value, found, failed = fetched(store, full, read)
    local outcome = series.MISS

    if found then
        outcome = series.HIT
    elseif failed then
        outcome = series.FAILED
    end

    series.asked(store.name, outcome, source().monotonic() - started)

    return value, found
end

--- Кладёт значение с тегами либо без них.
---@param store TntCacheStore
---@param key string
---@param value any
---@param ttl number|nil
---@param tags string[]|nil
local function stored(store, key, value, ttl, tags)
    present(value)
    store.driver.write(keyed(store, key), value, expiry(store, ttl), tags)
end

--- Кладёт под полный ключ, только если живого значения нет.
---
--- Чтение здесь — условие записи, и отказ драйвера бросает, а не
--- промахивается: промах означал бы «значения нет, пиши поверх чужого»,
--- и `add`, которым берут замок, отдавал бы замок второму владельцу.
--- Драйвер, умеющий это одним действием, решает сам: у драйвера по сети
--- между чтением и записью успел бы вклиниться сосед. Драйвер со вставкой
--- кладёт ею — просроченное к ней уже стёрто чтением.
---@param store TntCacheStore
---@param full string Ключ с приставкой
---@param value any
---@param ttl number|nil
---@param tags string[]|nil
---@return boolean added
local function absent(store, full, value, ttl, tags)
    local driver = store.driver

    if driver.add ~= nil then
        return driver.add(full, value, expiry(store, ttl), tags)
    end

    if select(2, alive(store, full, driver.read)) == true then
        return false
    end

    if driver.insert ~= nil then
        return driver.insert(full, value, expiry(store, ttl), tags)
    end

    driver.write(full, value, expiry(store, ttl), tags)

    return true
end

--- Кладёт по ключу, только если живого значения нет.
---@param store TntCacheStore
---@param key string
---@param value any
---@param ttl number|nil
---@param tags string[]|nil
---@return boolean added
local function placed(store, key, value, ttl, tags)
    present(value)

    return absent(store, keyed(store, key), value, ttl, tags)
end

--- Значение по ключу, а если его нет — вычисленное и сохранённое.
---
--- Чем спрашивать, решает вызывающий: `asked` — обращение, и оно ложится
--- в ряды; `fetched` — повторное чтение того же обращения под замком.
---@param store TntCacheStore
---@param key string
---@param ttl number|nil
---@param compute fun(): any
---@param tags string[]|nil
---@param look fun(store: TntCacheStore, full: string, read: fun(key: string): any, number|nil): any, boolean|nil
---@return any
local function computed(store, key, ttl, compute, tags, look)
    local value, found = look(store, keyed(store, key), store.driver.read)

    if found then
        return value
    end

    value = compute()
    stored(store, key, value, ttl, tags)

    return value
end

Module.outside_transaction = outside_transaction
Module.checked = checked
Module.keyed = keyed
Module.expiry = expiry
Module.alive = alive
Module.fetched = fetched
Module.asked = asked
Module.stored = stored
Module.absent = absent
Module.placed = placed
Module.computed = computed

return Module
