## Qué resuelve

Workflow con N generadores Seedance encadenados (`last_frame → first_frame`)
para superar el límite de 10–15 s del modelo. Si el audio dura menos que el
workflow completo, hay que saltarse los Seedances que sobran, y que el
`SaveVideo` final reciba igualmente el resultado.

La v2 de este pack bloqueaba con `ExecutionBlocker`. Funcionaba si el único
bloque caía al final, pero el `ExecutionBlocker` de ComfyUI se propaga de
forma irreversible por diseño del motor, así que cualquier rama bloqueada
aguas arriba de una cascada de concats terminaba bloqueando el `SaveVideo`.

La v3 cambia el modelo:

- El gate va sobre el **VIDEO de salida** del Seedance, no sobre la imagen
  de entrada.
- El input `video` del gate es **`lazy=True`**: si el gate está cerrado,
  ComfyUI no evalúa ese Seedance y no se gasta crédito de la API.
- La salida del gate es **`None`** en vez de `ExecutionBlocker` cuando la
  rama no aplica.
- Un solo `Video Concat Parallel` de hasta 12 slots filtra los `None` y
  concatena los que sí llegaron. Sin cascada.

## Nodos

### Duration Gate Video (Seed 1.5) — NUEVO, recomendado

Se conecta a la salida de cada Seedance:

```
[Seedance]      ─video──────┐
                             ├──> [Duration Gate Video] ──video──> [Video Concat Parallel]
[AudioDuration] ─duration───┘
                               min_duration = 4
```

- `duration >= 4` → evalúa el Seedance y pasa su video al concat.
- `duration <  4` → devuelve `None`; el Seedance **no se ejecuta**.

### Audio Pad To Min (Seed 1.5) — NUEVO

Rellena un `AUDIO` con silencio al final hasta llegar a `min_seconds`
(por defecto 4). Los clips que ya superan ese mínimo pasan tal cual.

Resuelve el caso típico del workflow de doblaje: `AudioCrop` corta
ventanas fijas de 10s y la última puede durar 2–3 s. Sin padding,
`AudioDuration` mide < 4, la `MathExpression a * (a >= 4)` lo pone
en 0 y el `DurationGateVideo` salta esa rama, perdiendo el segmento
final (un audio de 22 s terminaba dando un video de 20 s).

Cableado:

```
[AudioCrop] ──audio──> [AudioPadToMin] ──audio──┬──> [AudioDuration] ──> [MathExpression] ──> [Seedance.duration]
                                                 └──> [Apply WhisperX]   ──> [Seedance.prompt]
                       min_seconds = 4
```

Con el padder en medio, `AudioDuration` siempre devuelve ≥ 4, la rama
no se cierra y Seedance genera 4 s para ese chunk corto. La transcripción
no se ve afectada porque el silencio agregado va al final.

### Video Concat Parallel (Seed 1.5) — NUEVO, recomendado

12 slots `video_1..video_12` en paralelo, todos lazy. Filtra los inputs que
llegan como `None` y concatena el resto.

Outputs:
- `video`       — VIDEO concatenado (fallback a frames si el pack no
  expone `VideoFromComponents`).
- `frames`      — batch IMAGE `(N, H, W, C)` para mandar a `VHS_VideoCombine`
  junto con el audio master.
- `frame_count` — cantidad de frames totales.
- `fps`         — fps detectado del primer clip (24 por defecto).

### Duration Gate (Seed 1.5) — legacy

Mantenido para workflows antiguos. Actúa sobre IMAGE con `ExecutionBlocker`.
No recomendado en cascadas.

### Video Concat Filtered (Seed 1.5) — legacy

Mantenido para workflows antiguos con 4 slots. Sufre el problema de
propagación de `ExecutionBlocker`.

## Cableado recomendado (v3)

Para cada Seedance `i`:

1. `AudioCrop` de la ventana `i` → `AudioPadToMin (min_seconds=4)` →
   `AudioDuration` → `MathExpression ((a>0)*max(4,a))`. La salida del
   `AudioPadToMin` también va a `Apply WhisperX`.
2. El Seedance recibe:
   - `first_frame` = `last_frame` del Seedance anterior (o `LoadImage` si
     es el primero). Sin gate en medio.
   - `duration` = salida de la MathExpression.
   - `prompt`, `api_key`, etc.
3. `Seedance.video` → `DurationGateVideo.video`.
4. `MathExpression` → `DurationGateVideo.duration`.
5. `DurationGateVideo.video` → `VideoConcatParallel.video_i`.

Al final:

- `VideoConcatParallel.frames` + `VHS_LoadAudioUpload.audio` →
  `VHS_VideoCombine` (esta es la salida con audio real).
- Opcional: `VideoConcatParallel.video` → `SaveVideo` nativo (sin audio).

## Por qué funciona

La cadena `last_frame → first_frame` entre Seedances permanece intacta
porque los gates ya no la tocan. Cuando `AudioCrop[k]` se queda sin audio,
`AudioDuration[k..N]` devuelven 0, las `MathExpression[k..N]` devuelven 0,
los `DurationGateVideo[k..N]` cierran — nadie pide `video` → ComfyUI no
evalúa `Seedance[k..N]` → no hay crédito gastado. El `VideoConcatParallel`
recibe `video_1..video_{k-1}` con contenido y `video_k..video_N` como
`None`, concatena los primeros y el `SaveVideo` / `VHS_VideoCombine`
recibe siempre algo válido.

Con `AudioPadToMin` en medio, los chunks que estaban entre 0 y 4 s
(antes descartados) pasan a ser válidos: el padder los lleva a 4 s,
Seedance genera el video correspondiente y la concatenación final
respeta la duración real del audio.

## Instalación

1. Copia la carpeta a `ComfyUI/custom_nodes/`.
2. Reinicia ComfyUI.
3. Sin dependencias extra.

## Limitaciones conocidas

- El tipo `VIDEO` en ComfyUI no está estandarizado. El concat paralelo
  convierte todos los clips a frames (`IMAGE` batch) y redimensiona al tamaño
  del primero si hace falta. Para salida de máxima calidad con audio,
  usá siempre el camino `frames → VHS_VideoCombine` con el audio master.
- La cadena depende de que los gates cierren de forma **monotónica**:
  si se cerrara el gate `k` pero quedara abierto el `k+1`, `Seedance[k]`
  sería evaluado igualmente (porque `Seedance[k+1]` necesita su
  `last_frame`) aunque su video sea descartado por el gate `k`. En la
  práctica, como los `AudioCrop` son consecutivos, el patrón es monotónico.
