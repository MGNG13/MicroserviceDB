# 🍃 MicroserviceDBMongoManager — Web Edition

Aplicación web completa para exportar bases de datos MongoDB a JSON o copiar colecciones entre instancias MongoDB, con interfaz moderna en HTML/CSS/JS y comunicación en tiempo real vía WebSockets.

## Características

- **Modo Exportar**: Exporta cualquier base de datos/colección a archivos JSON estructurados.
- **Modo Copiar**: Transfiere colecciones entre servidores MongoDB con manejo inteligente de `_id`.
- **Interfaz moderna**: UI oscura, responsive, con progreso en tiempo real y logs.
- **X.509**: Soporte completo para autenticación con certificados.
- **Paginación robusta**: Maneja grandes volúmenes de datos sin timeouts.
- **Descarga ZIP**: Empaqueta todos los resultados en un solo archivo descargable.

## Instalación

```bash
# 1. Crear entorno virtual (recomendado Python 3.10–3.12)
python -m venv venv

# Windows
venv\Scripts\activate

# macOS/Linux
source venv/bin/activate

# 2. Instalar dependencias
pip install -r requirements.txt
```

## Uso

```bash
# Iniciar servidor
python app.py

# O con parámetros personalizados
python app.py --host 0.0.0.0 --port 8080
```

Abre tu navegador en **http://localhost:5000**

## Estructura del proyecto

```
mongodb_manager_web/
├── app.py                 # Servidor Flask + SocketIO + lógica de negocio
├── requirements.txt       # Dependencias
├── templates/
│   └── index.html         # Interfaz principal
└── static/
    ├── css/
    │   └── style.css      # Estilos modernos (tema oscuro)
    └── js/
        └── app.js         # Controlador frontend
```

## Flujo de trabajo

1. **Conectar**: Ingresa la URI de MongoDB origen (y destino si es modo copiar). Prueba la conexión.
2. **Seleccionar**: Elige las bases de datos y colecciones que deseas procesar.
3. **Ejecutar**: Inicia la operación y observa el progreso y logs en tiempo real.
4. **Resultados**: Revisa métricas, errores y descarga el ZIP con todos los archivos generados.

## Notas

- El servidor usa `threading` para operaciones largas sin bloquear la UI.
- Los certificados X.509 se suben temporalmente al servidor y se eliminan al cerrar.
- El modo copiar vacía colecciones existentes en destino y regenera los `_id` para evitar conflictos.

## Licencia

MIT — Hecho con ❤️ para administradores de MongoDB.
