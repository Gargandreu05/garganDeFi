import discord
from discord.ext import tasks, commands
import feedparser
import os
import conocimiento
import motor

# --- CONFIGURACIÓN DE DISCORD ---
intents = discord.Intents.default()
bot = commands.Bot(command_prefix='!', intents=intents)

# --- GESTIÓN DE ESTADO (MEMORIA) ---
ofertas_vistas = []

def cargar_historial():
    """Carga los enlaces ya procesados para no repetir."""
    global ofertas_vistas
    if not os.path.exists(conocimiento.HISTORY_FILE):
        return
    try:
        with open(conocimiento.HISTORY_FILE, 'r', encoding='utf-8') as f:
            ofertas_vistas = [line.strip() for line in f.readlines() if line.strip()]
        print(f"[MEMORIA] Historial cargado: {len(ofertas_vistas)} registros.")
    except Exception as e:
        print(f"[ERROR] Fallo al cargar historial: {e}")

def guardar_historial(link):
    """Guarda un nuevo enlace en el historial."""
    global ofertas_vistas
    ofertas_vistas.append(link)
    
    # Mantenimiento: Buffer circular de 500 items
    if len(ofertas_vistas) > 500:
        ofertas_vistas = ofertas_vistas[-500:]
    
    try:
        with open(conocimiento.HISTORY_FILE, 'w', encoding='utf-8') as f:
            for item in ofertas_vistas:
                f.write(f"{item}\n")
    except Exception as e:
        print(f"[ERROR] Fallo al escribir historial: {e}")

# --- EVENTOS DEL BOT ---

@bot.event
async def on_ready():
    print('=============================================')
    print(f' ROBOT: {bot.user}')
    print(' MODO:  SISTEMA EXPERTO (KBS) v10.0')
    print(f' IP:    {conocimiento.MI_IP}')
    print('=============================================')
    cargar_historial()
    if not buscar_chollos.is_running():
        buscar_chollos.start()

@tasks.loop(minutes=1)
async def buscar_chollos():
    channel = bot.get_channel(conocimiento.CHANNEL_ID)
    if not channel:
        print(f"[ERROR] Canal {conocimiento.CHANNEL_ID} no encontrado.")
        return

    print("[SCAN] Escaneando fuentes de conocimiento...")

    for fuente in conocimiento.FUENTES:
        nombre_fuente = fuente['nombre']
        url_rss = fuente['url']
        color_embed = fuente['color']
        bandera = fuente.get('flag', '')

        try:
            # Parseo del RSS
            feed = feedparser.parse(url_rss)
            if not feed.entries:
                continue

            # Invertir para procesar lo más antiguo primero (FIFO en el batch)
            entradas_ordenadas = feed.entries[::-1]

            for entrada in entradas_ordenadas:
                # 1. Filtro de Duplicados
                if entrada.link in ofertas_vistas:
                    continue
                
                # 2. Extracción y Evaluación
                # Pasamos también la descripción para buscar el precio anterior si no está en título
                descripcion = entrada.get('summary', '') or entrada.get('description', '')
                
                resultado = motor.InferenceEngine.evaluar_oferta(
                    entrada.title, descripcion, fuente=nombre_fuente
                )

                # 4. Actuación (Enviar o Ignorar)
                if resultado['decision']:
                    titulo_final = f"{bandera} {resultado['etiqueta']} {entrada.title}"
                    
                    embed = discord.Embed(
                        title=titulo_final,
                        url=entrada.link,
                        color=color_embed
                    )
                    
                    # CAMPO 1: PRECIO DESTACADO
                    precio_str = f"{resultado['precio_actual']}€" if resultado['precio_actual'] else "??"
                    embed.add_field(name="💶 Precio Final", value=f"**{precio_str}**", inline=True)
                    
                    # CAMPO 2: DESCUENTO (Solo si existe)
                    if resultado['descuento_porcentaje'] > 0:
                        embed.add_field(
                            name="🔥 Descuento", 
                            value=f"**-{resultado['descuento_porcentaje']}%**", 
                            inline=True
                        )
                    
                    # CAMPO 3: AHORRO (Solo si es significativo > 5€)
                    if resultado['ahorro_dinero'] > 5.0:
                        embed.add_field(
                            name="💰 Te ahorras", 
                            value=f"{resultado['ahorro_dinero']:.2f}€", 
                            inline=True
                        )

                    # CAMPO 4: DIAGNÓSTICO (Footer o Inline)
                    embed.add_field(name="🔍 Diagnóstico", value=resultado['razon'], inline=False)
                    
                    embed.set_footer(text=f"Fuente: {nombre_fuente} • Motor v10.0")

                    # Ping @everyone si es una alerta crítica (Error o VIP muy barato)
                    mensaje = ""
                    if '🚨' in resultado['etiqueta'] or '💎' in resultado['etiqueta']:
                        mensaje = "@everyone"

                    await channel.send(content=mensaje, embed=embed)
                    print(f"[APROBADO] {entrada.title} -> {resultado['razon']}")
                else:
                    # Log de descartes (útil para debug)
                    # print(f"[DESCARTADO] {entrada.title} -> {resultado['razon']}")
                    pass

                # Guardamos siempre para no re-evaluar
                guardar_historial(entrada.link)

        except Exception as e:
            print(f"[ERROR] Fallo en fuente {nombre_fuente}: {e}")
            continue

# Ejecución
if __name__ == "__main__":
    bot.run(conocimiento.TOKEN)
