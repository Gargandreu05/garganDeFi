import re
import math
import conocimiento

class InferenceEngine:
    
    @staticmethod
    def normalizar_texto(texto):
        return texto.lower().strip()

    @staticmethod
    def _analizar_contexto(texto_completo, start, end):
        """
        Analiza el contexto (palabras clave alrededor) de un hallazgo numérico.
        Retorna un diccionario de flags.
        """
        # Ventana de contexto: ~40 caracteres antes y después
        ctx_pre = texto_completo[max(0, start-40):start].lower()
        ctx_post = texto_completo[end:min(len(texto_completo), end+40)].lower()
        ctx_combined = f"{ctx_pre} {ctx_post}"

        flags = {
            'es_envio': False,
            'es_total_lote': False,
            'es_unitario': False,
            'es_original': False,  # MSRP / Precio Anterior
            'es_labeled': False    # Detectado explícitamente como "Precio: X"
        }

        # 0. Check Labeled (Si el número vino de un patrón "Precio: X", recibe prioridad)
        # Esto manejamos abajo en la extracción, pero aquí podemos reforzar si vemos "price:" pegado
        if re.search(r'(?:price|precio|cost|valor)[:\s]', ctx_pre[-15:]):
            flags['es_labeled'] = True

        # 1. Palabras de ENVÍO (Ignorar envío como precio de producto)
        terminos_envio = ['shipping', 'envío', 'envio', 'delivery', 'gastos', 'porte', 'plus', 'cost', '+']
        # Cuidado con '+', solo si está muy cerca
        if any(w in ctx_pre for w in terminos_envio) or any(w in ctx_post for w in terminos_envio):
            flags['es_envio'] = True

        # 2. Palabras de TOTAL / LOTE (Ignorar si buscamos precio unitario)
        terminos_total = ['total', 'lot', 'lote', 'pack', 'all', 'todo', 'set', 'bulk', 'sold', 'vendido', 'buying']
        if any(w in ctx_combined for w in terminos_total):
            flags['es_total_lote'] = True

        # 3. Palabras de UNITARIO (Priorizar)
        terminos_unit = ['price', 'precio', 'only', 'solo', 'each', '/ea', '/ud', 'unit', 'dimm', 'pc', 'stk', 'unidad', 'per']
        if any(w in ctx_combined for w in terminos_unit):
            flags['es_unitario'] = True

        # 4. Palabras de PRECIO ANTERIOR (Para detectar descuentos)
        # "Was $50", "Antes 50€", "PVP: 50"
        terminos_orig = ['was', 'antes', 'pvp', 'msrp', 'retail', 'usually', 'normally', 'old', 'street price', 'rrp', 'previous', 'bought for']
        if any(w in ctx_pre for w in terminos_orig): 
            flags['es_original'] = True

        return flags

    @classmethod
    def _extraer_candidatos(cls, texto):
        """
        Extrae todos los precios candidatos con su contexto.
        Soporta formatos: 135€, €135, $135, 135 USD, 135/DIMM, Price: 135
        """
        if not texto: return []
        
        candidatos = []
        
        # --- PATRÓN 1: CURRENCY (Standard) ---
        # Grupo A: Símbolo antes (e.g. $135, £20.50)
        # Grupo B: Símbolo después (e.g. 135€, 20 eur, 50 USD, 100 kr)
        patron_currency = r'(?:(?P<sym_pre>[\$€£¥])\s*(?P<val_pre>\d+(?:[.,]\d{1,2})?))|(?:(?P<val_post>\d+(?:[.,]\d{1,2})?)\s*(?P<sym_post>€|eur|euros?|usd|£|\$|gbp|aud|cad|cny|rmb|jpy|yen|kr|zl))'
        
        # --- PATRÓN 2: LABELED (Blog Style) ---
        # "Price: 150", "Cost: 15.5", "Valor: 500" (Sin moneda explícita)
        patron_labeled = r'(?:price|precio|cost|valor|only|solamente)\s*[:=]\s*(?P<val_labeled>\d+(?:[.,]\d{1,2})?)'

        # --- PATRÓN 3: UNIT SUFFIX (Formatos tipo "135/DIMM") ---
        # Si no hay moneda, pero hay sufijo de unidad claro.
        patron_unit = r'(?P<val_unit>\d+(?:[.,]\d{1,2})?)\s*/\s*(?P<unit>dimm|unit|ea|ud|pieza|stk|pc|gb|tb)'

        # Combinamos todo en un iterador para procesar ordenadamente
        # Nota: Ejecutamos las regex por separado para simplificar grupos
        
        def procesar_match(m, tipo):
            val_str = None
            if tipo == 'currency':
                val_str = m.group('val_pre') or m.group('val_post')
            elif tipo == 'labeled':
                val_str = m.group('val_labeled')
            elif tipo == 'unit':
                val_str = m.group('val_unit')
            
            if not val_str: return

            try:
                # Normalización "1.200,50" -> 1200.5
                clean_val = val_str.replace(',', '.')
                if clean_val.count('.') > 1:
                    clean_val = clean_val.replace('.', '', 1)
                
                valor = float(clean_val)
                if valor < 0.1: return

                start, end = m.span()
                flags = cls._analizar_contexto(texto, start, end)
                
                # Refuerzos según origen
                if tipo == 'labeled': flags['es_labeled'] = True
                if tipo == 'unit': flags['es_unitario'] = True

                candidatos.append({'valor': valor, 'flags': flags, 'src': tipo})
            except ValueError:
                pass

        for m in re.finditer(patron_currency, texto, re.IGNORECASE): procesar_match(m, 'currency')
        for m in re.finditer(patron_labeled, texto, re.IGNORECASE): procesar_match(m, 'labeled')
        for m in re.finditer(patron_unit, texto, re.IGNORECASE): procesar_match(m, 'unit')
        
        return candidatos

    @classmethod
    def extraer_precios_y_descuento(cls, titulo, descripcion=""):
        """
        Determina Precio Actual y Precio Original basándose en reglas contextuales.
        Jerarquía: Título > Descripción.
        """
        # 1. Obtener candidatos
        cand_titulo = cls._extraer_candidatos(titulo)
        cand_desc = cls._extraer_candidatos(descripcion)

        precio_actual = None
        precio_original = None

        # --- LÓGICA PRECIO ACTUAL ---
        def filtrar_buenos(lista):
            # Priorizamos Labeled o Unitario explícito, Penalizamos Envío/Total/MSRP
            return [c for c in lista 
                    if not c['flags']['es_envio'] 
                    and not c['flags']['es_total_lote'] 
                    and not c['flags']['es_original']]

        buenos_titulo = filtrar_buenos(cand_titulo)
        
        # A) TÍTULO MANDA
        if buenos_titulo:
            buenos_titulo.sort(key=lambda x: x['valor'])
            precio_actual = buenos_titulo[0]['valor']
        else:
            # B) Buscar en DESCRIPCIÓN
            buenos_desc = filtrar_buenos(cand_desc)
            if buenos_desc:
                buenos_desc.sort(key=lambda x: x['valor'])
                precio_actual = buenos_desc[0]['valor']
            else:
                # C) Fallback: Si todo es "lote", cogemos el menor lote (mientras no sea envío)
                fallbacks = [c for c in (cand_titulo + cand_desc) 
                             if c['flags']['es_total_lote'] and not c['flags']['es_envio']]
                if fallbacks:
                    fallbacks.sort(key=lambda x: x['valor'])
                    precio_actual = fallbacks[0]['valor']

        # --- LÓGICA PRECIO ORIGINAL (MSRP) ---
        # Solo si explícitamente parece original
        todos = cand_titulo + cand_desc
        candidatos_msrp = [c for c in todos if c['flags']['es_original']]
        
        if precio_actual and candidatos_msrp:
            # Debe ser mayor al actual
            validos_msrp = [c for c in candidatos_msrp if c['valor'] > precio_actual]
            if validos_msrp:
                # El mayor suele ser el PVP original
                validos_msrp.sort(key=lambda x: x['valor'], reverse=True)
                precio_original = validos_msrp[0]['valor']

        return precio_actual, precio_original

    @staticmethod
    def es_contenido_transaccional(titulo, fuente=""):
        t = titulo.lower()
        
        # 1. Chequeo Reddit Hardcore / Homelab
        if "reddit" in fuente.lower() or "homelab" in fuente.lower():
            if any(tag in t for tag in ['[w]', 'wtb', 'looking for', 'busco', 'compro', '[pc]', 'price check']):
                return False
            
            if "homelab" in fuente.lower() or "hardwareswap" in fuente.lower() or "sbcgaming" in fuente.lower():
                whitelist_tags = ['[fs]', '[o]', '[h]', 'selling', 'vendo', 'se vende']
                if not any(tag in t for tag in whitelist_tags):
                    return False

        # 2. Bloqueo de No-Venta
        if any(w in t for w in conocimiento.NON_TRANSACTIONAL_TERMS):
            return False
            
        # 3. Confirmación de Venta
        has_trigger = any(w in t for w in conocimiento.TRANSACTIONAL_TRIGGERS)
        has_price_symbol = bool(re.search(r'(?:€|eur|£|\$)', t))
        
        if not has_trigger and not has_price_symbol:
            return False
            
        return True

    @classmethod
    def evaluar_oferta(cls, titulo, descripcion, fuente=""):
        titulo_limpio = cls.normalizar_texto(titulo)
        
        if not cls.es_contenido_transaccional(titulo, fuente):
             return {
                'decision': False,
                'razon': "No es una venta (Blog/Noticia)",
                'etiqueta': '📰',
                'precio_actual': None, 'precio_original': None,
                'descuento_porcentaje': 0, 'ahorro_dinero': 0.0
            }

        precio_actual, precio_orig = cls.extraer_precios_y_descuento(titulo, descripcion)
        
        resultado = {
            'decision': False,
            'razon': "Descarte",
            'etiqueta': '❌',
            'precio_actual': precio_actual,
            'precio_original': precio_orig,
            'descuento_porcentaje': 0,
            'ahorro_dinero': 0.0
        }

        if precio_actual and precio_orig and precio_orig > precio_actual:
            dto = ((precio_orig - precio_actual) / precio_orig) * 100
            resultado['descuento_porcentaje'] = int(dto)
            resultado['ahorro_dinero'] = precio_orig - precio_actual

        for termino in conocimiento.BLACKLIST_TERMS:
            if termino in titulo_limpio:
                resultado['razon'] = "Blacklist"
                resultado['etiqueta'] = '⛔'
                return resultado

        aprobado = False
        
        for err in conocimiento.KEYWORDS_ERROR:
            if err in titulo_limpio:
                resultado['razon'] = "Posible Error"
                resultado['etiqueta'] = '🚨'
                aprobado = True; break
        
        if not aprobado:
            if any(w in titulo_limpio for w in conocimiento.FACTORY_TERMS):
                resultado['razon'] = "Formato Mayorista/OEM"
                resultado['etiqueta'] = '🏭'
                aprobado = True
            elif any(w in titulo_limpio for w in conocimiento.CHINA_VIP):
                resultado['razon'] = "Importación China"
                resultado['etiqueta'] = '🇨🇳'
                aprobado = True
            elif any(w in titulo_limpio for w in conocimiento.SOFTWARE_VIP):
                resultado['razon'] = "Software VIP"
                resultado['etiqueta'] = '💾'
                aprobado = True
            elif any(w in titulo_limpio for w in conocimiento.HARDWARE_VIP):
                resultado['razon'] = "Hardware VIP"
                resultado['etiqueta'] = '💎'
                aprobado = True
                if precio_actual and 2.0 < precio_actual < 15.0:
                    resultado['razon'] = "Posible Precio de Coste"

            elif any(w in titulo_limpio for w in conocimiento.GAMING_VIP):
                resultado['razon'] = "Gaming VIP"
                resultado['etiqueta'] = '👾'
                aprobado = True

            if not aprobado and precio_actual is not None:
                if precio_actual < conocimiento.REGLAS_PRECIO['MAX_PRECIO_GANGA']:
                    resultado['razon'] = "Ganga (<5€)"
                    resultado['etiqueta'] = '📉'
                    aprobado = True
                elif resultado['descuento_porcentaje'] > conocimiento.REGLAS_PRECIO['MIN_DESCUENTO_TOP']:
                     resultado['razon'] = f"Gran Descuento {resultado['descuento_porcentaje']}%"
                     resultado['etiqueta'] = '🏷️'
                     aprobado = True

        if not aprobado and "Reddit" in fuente and "BuildAPC" in fuente:
             resultado['razon'] = "Reddit Hardware"
             resultado['etiqueta'] = '👽'
             aprobado = True

        resultado['decision'] = aprobado
        return resultado
