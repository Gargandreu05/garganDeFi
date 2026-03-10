import socket
import os

# --- BASE DE CONOCIMIENTO (KNOWLEDGE BASE) ---

# 1. Utilidades de Entorno
def obtener_ip_local():
    """Detecta la IP local para configurar fuentes dinámicas."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"

MI_IP = obtener_ip_local()

# 2. Configuración Global
TOKEN = os.getenv('DISCORD_TOKEN')
CHANNEL_ID = 1469016669811900569
HISTORY_FILE = 'historial.txt'

# 3. Fuentes RSS
FUENTES = [
    # --- TIER -1: HARDWARE PURO & MANUFACTURA (La "Deep Web" del Hardware) ---
    {'nombre': 'China-Gadgets (DE)', 'url': 'https://www.china-gadgets.de/feed/', 'color': 0xE60012, 'lang': 'de', 'flag': '🇨🇳'},
    {'nombre': 'CNX Software (Dev)', 'url': 'https://www.cnx-software.com/feed/', 'color': 0x228B22, 'lang': 'en', 'flag': '🛠️'},
    {'nombre': 'Reddit HomelabSales', 'url': 'https://www.reddit.com/r/homelabsales/new/.rss', 'color': 0x333333, 'lang': 'en', 'flag': '🏗️'},
    {'nombre': 'Reddit SBC Gaming', 'url': 'https://www.reddit.com/r/SBCGaming/new/.rss', 'color': 0x9B59B6, 'lang': 'en', 'flag': '👾'},
    {'nombre': 'LowEndTalk (VPS)', 'url': 'https://lowendtalk.com/categories/offers/feed.rss', 'color': 0x34495E, 'lang': 'en', 'flag': '🗑️'},

    # --- TIER 0: CHINA & IMPORTACIÓN (Precio > Rapidez) ---
    {'nombre': 'Chollometro China', 'url': 'https://www.chollometro.com/rss/tag/china', 'color': 0xFF0000, 'lang': 'es', 'flag': '🇨🇳'},
    {'nombre': 'MyDealz Import (DE)', 'url': 'https://www.mydealz.de/rss/gruppe/china-elektronik', 'color': 0x000000, 'lang': 'de', 'flag': '🏯'},
    {'nombre': 'PandaCheck Gadgets', 'url': 'https://www.pandacheck.com/rss/blog', 'color': 0x00FF00, 'lang': 'en', 'flag': '🐼'},
    
    # --- TIER 1: REACONDICIONADOS & ARBITRAJE ---
    {'nombre': 'Warehouse & Refurb', 'url': 'https://www.chollometro.com/rss/tag/reacondicionados', 'color': 0xFFA500, 'lang': 'es', 'flag': '♻️'},
    {'nombre': 'eBay Europa (RTX)', 'url': 'https://www.ebay.es/sch/i.html?_nkw=(rtx+40*,rtx+30*,rx+7*)&_sop=10&LH_BIN=1&_rss=1', 'color': 0xE53238, 'lang': 'es', 'flag': '📦'},
    {'nombre': 'Mindfactory Alerts', 'url': 'https://www.mydealz.de/rss/keyword/mindfactory', 'color': 0x0026ff, 'lang': 'de', 'flag': '🏭'},
    {'nombre': 'Amazon Warehouse (DE)', 'url': 'https://www.mydealz.de/rss/keyword/amazon-warehouse', 'color': 0xFF9900, 'lang': 'de', 'flag': '🇩🇪'},
    
    # AGREGADORES
    {'nombre': 'Chollometro', 'url': 'https://www.chollometro.com/rss/nuevos', 'color': 0xFF5733, 'lang': 'es', 'flag': '🇪🇸'},
    {'nombre': 'Dealabs (FR)', 'url': 'https://www.dealabs.com/rss/groupe/high-tech', 'color': 0x035985, 'lang': 'fr', 'flag': '🇫🇷'},
    {'nombre': 'HotUKDeals (UK)', 'url': 'https://www.hotukdeals.com/rss/group/gaming', 'color': 0x0000FF, 'lang': 'en', 'flag': '🇬🇧'},

    # NICHO
    {'nombre': 'Reddit BuildAPC', 'url': 'https://www.reddit.com/r/buildapcsales/new/.rss', 'color': 0xFF4500, 'lang': 'en', 'flag': '👽'},

    # LOCAL
    {'nombre': 'Telegram Local', 'url': f"http://{MI_IP}:3000/?action=display&bridge=TelegramBridge&username=chollometro&format=Atom", 'color': 0x0088cc, 'lang': 'es', 'flag': '✈️'}
]

# 4. HECHOS Y REGLAS (Facts & Rules Database)

# Hechos Negativos: RECHAZO TOTAL (Blacklist Profunda)
BLACKLIST_TERMS = [
    # Ocio Pasivo
    'cine', 'mubi', 'netflix', 'disney', 'prime video', 'hbo', 'película', 'pelicula', 'bluray', 'dvd', 'cinema', 'filmin',
    'entrada', 'ticket', 'entradas',
    # Deportes
    'formula e', 'fórmula e', 'f1', 'fútbol', 'partido', 'dazn', 'laliga', 'nba', 'champions', 'camiseta equipo', 'balón',
    # Ropa / Moda / Super
    'ropa', 'camiseta', 'pantalón', 'zapatillas', 'moda', 'fashion', 'zara', 'nike air', 'jordan', 'adidas',
    'supermercado', 'comida', 'bebida', 'café', 'nespresso', 'detergente', 'champu',
    'funko', 'pop', 'muñeco', 'juguete',
    # Basura General
    'sorteo', 'recopilación', 'selección', 'consultorio', 'duda', 'ayuda', 'financiación',
    'ikea', 'shein', 'temu', 'aliexpress', 'miravia',
    'cupón', 'cupon', 'gutschein', 'gewinnspiel', 'concours', 'giveaway'
]

# LISTA NUEVA: WHOLESALE & FACTORY TERMS (Etiqueta 🏭) - NUEVA
FACTORY_TERMS = [
    'bulk', 'oem', 'tray', 'sin caja', 'no box', 'refurbished', 'reacondicionado', 
    'open box', 'desprecintado', 'server pull', 'enterprise gear', 'xeon', 'epyc', 
    'ecc reg', 'white label'
]

# LISTA VIP 1: HARDWARE (Etiqueta 💎)
HARDWARE_VIP = [
    'rtx', 'gtx', 'rx 7', 'rx 6', 'amd', 'intel', 'ryzen', 'core i5', 'core i7', 'core i9', 'threadripper',
    'gpu', 'cpu', 'motherboard', 'placa base', 'psu', 'ram', 'ddr4', 'ddr5',
    'server', 'servidor', 'nas', 'synology', 'qnap', 'ubiquiti', 'unifi', 'mikrotik', 'cisco', 'switch', 'router',
    'ssd', 'nvme', 'm.2', 'disco duro', 'hdd',
    'monitor', 'ultrawide', 'oled', 'qled', 'ips'
]

# LISTA VIP 2: CHINA TECH & IOT (Etiqueta 🇨🇳) - NUEVA
CHINA_VIP = [
    'xiaomi', 'poco', 'oneplus', 'realme', 
    'creality', 'bambu lab', 'anycubic', 'elegoo', 'ender', 'prusa',
    'firebat', 'chatreey', 'beelink', 'minisforum', 'gmktec', 'nuc',
    'anbernic', 'miyoo', 'powkiddy', 'retroid',
    'flipper zero', 'hak5', 'proxmark',
    'sipeed', 'lilygo', 'esp32', 'arduino', 'raspberry', 'orange pi',
    'netac', 'kingspec', 'fanxiang', 'zeuslap', 'szbox', 'chatreey', 
    'machinist', 'huananzhi', 'soyoyo'
]

# LISTA VIP 3: SOFTWARE & DEV (Etiqueta 💾)
SOFTWARE_VIP = [
    'chatgpt', 'openai', 'copilot', 'api', 'gemini', 'claude',
    'jetbrains', 'intellij', 'pycharm', 'webstorm', 'ide', 'visual studio',
    'vpn', 'vps', 'hosting', 'dominio', 'domain', 'cloud', 'aws', 'azure', 'google cloud',
    'python', 'java', 'docker', 'kubernetes', 'linux', 'ubuntu',
    'udemy', 'coursera', 'edx', 'platzi', 'curso', 'bootcamp',
    'windows', 'office', 'licencia', 'key', 'antivirus'
]

# LISTA VIP 4: GAMING (Etiqueta 👾)
GAMING_VIP = [
    'steam', 'steam deck', 'valve', 'epic games', 'juego gratis', 'free game',
    'ps5', 'playstation 5', 'xbox', 'series x',
    'nintendo', 'switch', 'joycon', 'pro controller',
    'logitech', 'razer', 'corsair', 'teclado mecánico', 'mechanical keyboard', 'mouse'
]

KEYWORDS_ERROR = ['error', 'bug', 'glitch', 'fallo', 'corred', 'liquidación', 'errata', 'preisfehler', 'erreur', 'bug de prix', 'price mistake']

# NUEVO: FILTROS SEMÁNTICOS ANTI-RUIDO
NON_TRANSACTIONAL_TERMS = [
    'review', 'análisis', 'opinion', 'tutorial', 'guide', 'guía', 'help', 'ayuda', 
    'question', 'pregunta', 'issue', 'problem', 'problema', 'rumor', 'leak', 
    'filtración', 'announced', 'anunciado', 'discussion', 'discusión', 'wtb', 
    'looking for', 'busco', 'compro', 'price check', 'pc]', 'how to', 'cómo'
]

TRANSACTIONAL_TRIGGERS = [
    'buy', 'comprar', 'sale', 'venta', 'sold', 'vendo', 'offer', 'oferta', 
    'coupon', 'cupon', 'cupón', 'discount', 'descuento', 'code', 'código', 
    'price', 'precio', 'shipping', 'envío', 'stock', 'fs]', '[fs]', 
    'for sale', 'selling'
]

REGLAS_PRECIO = {
    'MAX_PRECIO_GANGA': 5.0,   # Bajado a 5€ para detectar chollitos chinos (cables/adaptadores)
    'MIN_DESCUENTO_TOP': 40.0
}
