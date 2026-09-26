rockspec_format = '3.0'

package = 'tnt-cache'
version = 'scm-1'

source = {
    url = 'git+https://github.com/tnt-skein/tnt-cache.git',
    branch = 'main',
}

description = {
    summary = 'Кэш приложения: значения по ключу на срок, теги и замки, в памяти, в спейсе или в Redis',
    detailed = [[
        Значение по ключу на срок: get, put, add, remember, forget, pull,
        increment, flush, sweep; срок в секундах, приставка ключей.
        Просроченное читается как отсутствующее и стирается по дороге
        пишущим узлом, а sweep чистит заранее — кусками с уступкой,
        партиями по GT с limit, чтобы обход большого спейса не упирался
        в срез файбера.

        Теги у записи и забывание по тегу без обхода всего кэша; замок
        на ключ со сроком и меткой держателя, с ожиданием и без;
        remember с замком считает значение один раз, остальные ждут.

        Драйверы: memory — таблица в процессе, space — спейс Tarantool
        с шагом миграции, redis — Redis через клиент tnt-redis,
        пришедший аргументом: срок ведёт сервер (PXAT), add, pull
        и increment — одной командой. Свой драйвер — таблица функций
        по договору из документа.

        Договор отказа назван у каждого действия: чтение при отказе
        драйвера отвечает промахом и пишет о нём в журнал, запись
        бросает, sweep отдаёт пару.

        Обращения за значением видны рядами метрик: сколько попаданий,
        промахов и отказов было у каждого хранилища и сколько шло чтение
        драйвером.

        Зависит от tnt-must (броски ошибок программиста), tnt-clock
        (часы), tnt-log (журнал промахов), tnt-metrics (ряды обращений)
        и tnt-external (подмена часов, уступки и транзакции в проверках).
    ]],
    homepage = 'https://github.com/tnt-skein/tnt-cache',
    issues_url = 'https://github.com/tnt-skein/tnt-cache/issues',
    maintainer = 'tnt-skein',
    license = 'MIT',
    labels = { 'tarantool', 'cache', 'redis', 'ttl', 'lock' },
}

dependencies = {
    'lua >= 5.1',
    -- Броски ошибок программиста словом, без места в коде.
    'tnt-must',
    -- Стенные часы срока, монотонные часы и пауза ожидания замка.
    'tnt-clock',
    -- Журнал промахов по отказу драйвера с подавлением повторов.
    'tnt-log',
    -- Подмена уступки, часов, метки держателя и транзакции в проверках.
    'tnt-external',
    -- Ряды обращений в реестре встроенного metrics: итог и длительность.
    'tnt-metrics',
}

build = {
    type = 'builtin',
    modules = {
        ['tnt.cache'] = 'tnt/cache.lua',
        ['tnt.cache.lock'] = 'tnt/cache/lock.lua',
        ['tnt.cache.memory'] = 'tnt/cache/memory.lua',
        ['tnt.cache.record'] = 'tnt/cache/record.lua',
        ['tnt.cache.redis'] = 'tnt/cache/redis.lua',
        ['tnt.cache.series'] = 'tnt/cache/series.lua',
        ['tnt.cache.space'] = 'tnt/cache/space.lua',
        ['tnt.cache.store'] = 'tnt/cache/store.lua',
        ['tnt.cache.tagged'] = 'tnt/cache/tagged.lua',
    },
}
