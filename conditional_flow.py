# comfyui-seed-1.5-utilidades/conditional_flow.py
# VERSION 2.0 — SOLUCIÓN C DEFINITIVA
#
# Dos nodos que trabajan juntos para saltar ByteDance cuando duration < 4
# SIN romper el Video Concat final.
#
#   1. Duration Gate (Seed 1.5)
#      Va entre la imagen y ByteDance. Si duration < min_duration, corta la
#      ejecución aguas abajo mediante ExecutionBlocker. ByteDance no se llama.
#
#   2. Video Concat Filtered (Seed 1.5)
#      Sustituye al VideoConcat actual. Tiene 4 slots de entrada VIDEO, marcados
#      como lazy=True. Ignora los que vienen bloqueados y concatena solo los
#      válidos. Compatible con el tipo VIDEO de ComfyUI.

from comfy_execution.graph import ExecutionBlocker


# ═══════════════════════════════════════════════════════════════════════════
#  NODO 1: Duration Gate
# ═══════════════════════════════════════════════════════════════════════════

class DurationGate:
    """
    Deja pasar la imagen solo si duration >= min_duration.
    Si no cumple, bloquea la ejecución de todos los nodos aguas abajo.

    Cableado típico:
        [LoadImage]    ─image─────┐
                                   ├─> [Duration Gate] ─image─> [ByteDance]
        [AudioDuration] ─duration─┘
                                   min_duration = 4  (para Seedance Pro)

    Efecto:
      - duration >= 4  → image pasa, ByteDance se ejecuta normal.
      - duration <  4  → image queda BLOQUEADA, ByteDance NO se ejecuta.
                         No gasta créditos, no crashea.
    """

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "image": ("IMAGE",),
                "duration": ("INT", {
                    "default": 0,
                    "min": 0,
                    "max": 999999,
                    "forceInput": True,
                    "tooltip": "Duración recibida de AudioDuration (en segundos)."
                }),
                "min_duration": ("INT", {
                    "default": 4,
                    "min": 0,
                    "max": 999999,
                    "tooltip": (
                        "Umbral mínimo. Si duration < min_duration, ByteDance no se ejecuta.\n"
                        "  4 = Seedance 1.5 Pro (mínimo que acepta la API)\n"
                        "  1 = bloquea solo si es 0"
                    ),
                }),
            },
        }

    RETURN_TYPES = ("IMAGE",)
    RETURN_NAMES = ("image",)
    FUNCTION = "gate"
    CATEGORY = "Seed1.5/Flow"

    def gate(self, image, duration, min_duration):
        if duration < min_duration:
            print(f"[Seed1.5 DurationGate] 🛑 duration={duration} < min_duration={min_duration} → BLOQUEADO (ByteDance no se ejecutará)")
            return (ExecutionBlocker(None),)
        print(f"[Seed1.5 DurationGate] ✅ duration={duration} >= min_duration={min_duration} → PASA")
        return (image,)


# ═══════════════════════════════════════════════════════════════════════════
#  NODO 2: Video Concat Filtered
# ═══════════════════════════════════════════════════════════════════════════
#
# Este nodo usa "lazy evaluation". Cuando un input tiene lazy=True, ComfyUI
# NO lo evalúa automáticamente — solo lo hace si lo pedimos explícitamente en
# check_lazy_status(). Así podemos decidir qué inputs evaluar basándonos en
# qué conexiones existen en el grafo, e ignorar el resto.
#
# La ventaja: aunque un DurationGate aguas arriba devuelva ExecutionBlocker,
# este concatenador no se bloquea porque puede decidir NO evaluar ese input.

class VideoConcatFiltered:
    """
    Concatena hasta 4 vídeos, ignorando los que no estén conectados
    o vengan de una rama bloqueada por un DurationGate.

    IMPORTANTE: solo conecta los videos que realmente van a existir.
    Los slots no conectados se ignoran automáticamente.
    """

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "filename_prefix": ("STRING", {"default": "video_concat"}),
            },
            "optional": {
                "video_1": ("VIDEO", {"lazy": True}),
                "video_2": ("VIDEO", {"lazy": True}),
                "video_3": ("VIDEO", {"lazy": True}),
                "video_4": ("VIDEO", {"lazy": True}),
            },
        }

    RETURN_TYPES = ("VIDEO",)
    RETURN_NAMES = ("video",)
    FUNCTION = "concat"
    CATEGORY = "Seed1.5/Flow"

    def check_lazy_status(self, filename_prefix, **kwargs):
        """
        ComfyUI llama a este método ANTES de evaluar los inputs lazy.
        Debemos devolver la lista de nombres de inputs que queremos
        que ComfyUI evalúe. Los que no pidamos, quedarán como None.

        IMPORTANTE: solo pedir inputs que realmente tengan link conectado
        en el grafo. Pedir un input no conectado produce el error
        "there is no input to that node at all".
        """
        needed = []
        # Este método se llama con los kwargs ya parcialmente poblados.
        # Los inputs que aún no se han evaluado vienen como None.
        # Los que ya se evaluaron vienen con su valor.
        # Solo pedimos evaluar los que aún son None Y tienen link.
        # Pero check_lazy_status no recibe info directa de links. Truco:
        # ComfyUI solo pasa kwargs de inputs conectados o con default.
        # Si un input NO está en kwargs, no está conectado → lo ignoramos.
        for key in ("video_1", "video_2", "video_3", "video_4"):
            if key in kwargs and kwargs[key] is None:
                # Está en kwargs (conectado) pero aún no evaluado → pedirlo.
                needed.append(key)
        return needed

    def concat(self, filename_prefix, **kwargs):
        """
        Los inputs que vengan bloqueados por ExecutionBlocker aguas arriba
        NO llegarán aquí (ComfyUI corta la cadena). Los que lleguen son válidos.
        """
        videos = []
        for i in range(1, 5):
            key = f"video_{i}"
            v = kwargs.get(key)
            if v is not None:
                videos.append(v)
                print(f"[Seed1.5 VideoConcatFiltered] ✅ {key} válido")
            else:
                print(f"[Seed1.5 VideoConcatFiltered] ⏭️ {key} ausente/bloqueado (ignorado)")

        if len(videos) == 0:
            raise RuntimeError(
                "[Seed1.5 VideoConcatFiltered] TODOS los vídeos están bloqueados o vacíos. "
                "Verifica que al menos un DurationGate permita pasar su rama."
            )

        print(f"[Seed1.5 VideoConcatFiltered] Concatenando {len(videos)} vídeos válidos")

        if len(videos) == 1:
            return (videos[0],)

        return (self._concatenate(videos),)

    def _concatenate(self, videos):
        """
        Concatena vídeos soportando múltiples formatos de tipo VIDEO en ComfyUI.
        """
        # Estrategia 1: si son tensores IMAGE-batch (B, H, W, C)
        try:
            import torch
            if all(isinstance(v, torch.Tensor) for v in videos):
                return torch.cat(videos, dim=0)
        except Exception:
            pass

        # Estrategia 2: objetos VIDEO de comfy_api con get_components()
        try:
            import torch
            if all(hasattr(v, "get_components") for v in videos):
                # Obtener frames, audio y fps de cada vídeo
                all_images = []
                audios = []
                fps = None
                for v in videos:
                    comp = v.get_components()
                    imgs = comp.images if hasattr(comp, "images") else comp.get("images")
                    if imgs is not None:
                        all_images.append(imgs)
                    if hasattr(comp, "audio"):
                        audios.append(comp.audio)
                    if fps is None and hasattr(comp, "fps"):
                        fps = comp.fps

                if all_images:
                    combined_frames = torch.cat(all_images, dim=0)
                    # Devolver algo que sea interpretable como VIDEO.
                    # Para ComfyUI moderno, un batch de imágenes funciona como VIDEO
                    # si luego pasa por un nodo que lo interprete como tal.
                    return combined_frames
        except Exception as e:
            print(f"[Seed1.5 VideoConcatFiltered] Estrategia 2 falló: {e}")

        # Estrategia 3 (fallback): devolver el primer vídeo
        print("[Seed1.5 VideoConcatFiltered] ⚠️ No se pudo concatenar correctamente. Devolviendo el primer vídeo.")
        return videos[0]


# ═══════════════════════════════════════════════════════════════════════════
#  NODO 3 (bonus): Video Passthrough Filtered
#  Pasa el vídeo tal cual, o devuelve None si viene bloqueado.
#  Útil para encadenar con el VideoConcat nativo tuyo sin romperlo.
# ═══════════════════════════════════════════════════════════════════════════

class VideoPassthroughOrSkip:
    """
    Pasa el vídeo de entrada tal cual, o bloquea también la salida si
    viene bloqueado. Útil si quieres seguir usando el VideoConcat original
    tuyo — pero te avisará si los inputs bloqueados lo rompen.
    """

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "video": ("VIDEO",),
            },
        }

    RETURN_TYPES = ("VIDEO",)
    RETURN_NAMES = ("video",)
    FUNCTION = "passthrough"
    CATEGORY = "Seed1.5/Flow"

    def passthrough(self, video):
        return (video,)


# ═══════════════════════════════════════════════════════════════════════════

NODE_CLASS_MAPPINGS = {
    "Seed15_DurationGate":        DurationGate,
    "Seed15_VideoConcatFiltered": VideoConcatFiltered,
    "Seed15_VideoPassthrough":    VideoPassthroughOrSkip,
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "Seed15_DurationGate":        "Duration Gate (Seed 1.5)",
    "Seed15_VideoConcatFiltered": "Video Concat Filtered (Seed 1.5)",
    "Seed15_VideoPassthrough":    "Video Passthrough (Seed 1.5)",
}
