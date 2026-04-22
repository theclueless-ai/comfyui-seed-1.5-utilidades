# comfyui-seed-utilidades/conditional_flow.py
# VERSION 3.0 - Gate sobre VIDEO con sentinela None + concat paralelo
#
# Motivo del rediseño (ver README):
#   ComfyUI propaga ExecutionBlocker de forma irreversible ("There is
#   intentionally no way to stop an ExecutionBlocker from propagating
#   forward"). Si un DurationGate bloquea una rama con ExecutionBlocker
#   y esa rama alimenta un concat, el concat se bloquea; si los concats
#   están en cascada, la cascada entera muere y SaveVideo no recibe nada.
#
#   Solución: el gate evalúa lazy y devuelve None (no ExecutionBlocker)
#   cuando la rama no aplica. Un único concat paralelo de N slots filtra
#   los None y concatena los que sí llegaron.

from comfy_execution.graph import ExecutionBlocker


# ═══════════════════════════════════════════════════════════════════════════
#  NODO 1: Duration Gate (imagen) — compatible hacia atrás
# ═══════════════════════════════════════════════════════════════════════════

class DurationGate:
    """
    Gate sobre IMAGE basado en duración. Mantenido por compatibilidad con
    workflows viejos. Para videos largos preferir DurationGateVideo.

    duration >= min_duration → pasa la imagen.
    duration <  min_duration → ExecutionBlocker (ByteDance no se ejecuta).
    """

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "image": ("IMAGE",),
                "duration": ("INT", {
                    "default": 0, "min": 0, "max": 999999, "forceInput": True,
                    "tooltip": "Duración recibida de AudioDuration (en segundos).",
                }),
                "min_duration": ("INT", {
                    "default": 4, "min": 0, "max": 999999,
                    "tooltip": "Umbral mínimo. Si duration < min_duration, la rama se bloquea.",
                }),
            },
        }

    RETURN_TYPES = ("IMAGE",)
    RETURN_NAMES = ("image",)
    FUNCTION = "gate"
    CATEGORY = "Seed1.5/Flow"

    def gate(self, image, duration, min_duration):
        if duration < min_duration:
            print(f"[DurationGate] duration={duration} < {min_duration} → BLOCK")
            return (ExecutionBlocker(None),)
        return (image,)


# ═══════════════════════════════════════════════════════════════════════════
#  NODO 2 (NUEVO): Duration Gate Video — lazy, con sentinela None
# ═══════════════════════════════════════════════════════════════════════════

class DurationGateVideo:
    """
    Gate sobre VIDEO que NO usa ExecutionBlocker. Cuando la duración es
    insuficiente, devuelve None y además NO evalúa el input `video` gracias
    a lazy=True (no llama al Seedance, no gasta crédito).

    Cableado típico:
        [Seedance] ─video────────┐
                                  ├─> [Duration Gate Video] ─video─> [VideoConcatParallel]
        [AudioDuration] ─duration┘

    - duration >= min_duration → evalúa Seedance y pasa su video.
    - duration <  min_duration → devuelve None. Seedance NO se ejecuta.
    """

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "duration": ("INT", {
                    "default": 0, "min": 0, "max": 999999, "forceInput": True,
                    "tooltip": "Duración recibida de AudioDuration (en segundos).",
                }),
                "min_duration": ("INT", {
                    "default": 4, "min": 0, "max": 999999,
                    "tooltip": "Umbral mínimo. Si duration < min_duration, el video no se evalúa.",
                }),
            },
            "optional": {
                "video": ("VIDEO", {"lazy": True}),
            },
        }

    RETURN_TYPES = ("VIDEO",)
    RETURN_NAMES = ("video",)
    FUNCTION = "gate"
    CATEGORY = "Seed1.5/Flow"

    def check_lazy_status(self, duration, min_duration, video=None):
        if duration >= min_duration and video is None:
            return ["video"]
        return []

    def gate(self, duration, min_duration, video=None):
        if duration < min_duration:
            print(f"[DurationGateVideo] duration={duration} < {min_duration} → SKIP (None)")
            return (None,)
        return (video,)


# ═══════════════════════════════════════════════════════════════════════════
#  NODO 3: Video Concat Filtered (compat — cascada vieja)
# ═══════════════════════════════════════════════════════════════════════════

class VideoConcatFiltered:
    """
    Mantenido por compatibilidad. Para workflows nuevos usar
    VideoConcatParallel. Esta versión sólo filtra None; los ExecutionBlocker
    aguas arriba siguen propagándose (limitación del motor de ComfyUI).
    """

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {"filename_prefix": ("STRING", {"default": "video_concat"})},
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
        needed = []
        for key in ("video_1", "video_2", "video_3", "video_4"):
            if key in kwargs and kwargs[key] is None:
                needed.append(key)
        return needed

    def concat(self, filename_prefix, **kwargs):
        videos = [kwargs[f"video_{i}"] for i in range(1, 5)
                  if kwargs.get(f"video_{i}") is not None]
        if not videos:
            raise RuntimeError("VideoConcatFiltered: no hay vídeos válidos.")
        if len(videos) == 1:
            return (videos[0],)
        return (_concat_videos(videos),)


# ═══════════════════════════════════════════════════════════════════════════
#  NODO 4 (NUEVO): Video Concat Parallel — N slots en paralelo, filtra None
# ═══════════════════════════════════════════════════════════════════════════

MAX_PARALLEL_VIDEOS = 12


class VideoConcatParallel:
    """
    Concatena hasta MAX_PARALLEL_VIDEOS videos en PARALELO (no cascada).
    Filtra los inputs que llegan como None (gate cerrado aguas arriba).

    Ventajas sobre VideoConcatFiltered:
      - No cascada → un gate cerrado en la rama 3 no contamina las ramas 4+.
      - Hasta 12 slots en un único nodo, el canvas queda limpio.
      - Salida consistente: siempre batch de frames (N, H, W, C) como IMAGE
        para luego combinar con audio en VHS_VideoCombine, además del VIDEO
        concatenado para compatibilidad.
    """

    @classmethod
    def INPUT_TYPES(cls):
        optional = {
            f"video_{i}": ("VIDEO", {"lazy": True})
            for i in range(1, MAX_PARALLEL_VIDEOS + 1)
        }
        return {
            "required": {},
            "optional": optional,
        }

    RETURN_TYPES = ("VIDEO", "IMAGE", "INT", "FLOAT")
    RETURN_NAMES = ("video", "frames", "frame_count", "fps")
    FUNCTION = "concat"
    CATEGORY = "Seed1.5/Flow"

    def check_lazy_status(self, **kwargs):
        needed = []
        for i in range(1, MAX_PARALLEL_VIDEOS + 1):
            key = f"video_{i}"
            if key in kwargs and kwargs[key] is None:
                needed.append(key)
        return needed

    def concat(self, **kwargs):
        videos = []
        for i in range(1, MAX_PARALLEL_VIDEOS + 1):
            v = kwargs.get(f"video_{i}")
            if v is not None:
                videos.append(v)
                print(f"[VideoConcatParallel] video_{i} OK")
            else:
                print(f"[VideoConcatParallel] video_{i} vacío/gate cerrado (skip)")

        if not videos:
            raise RuntimeError(
                "VideoConcatParallel: TODAS las ramas están vacías. "
                "Verifica que al menos un DurationGateVideo permita pasar su rama."
            )

        frames, fps = _stack_frames_and_fps(videos)
        video_out = videos[0] if len(videos) == 1 else _concat_videos(videos)
        return (video_out, frames, int(frames.shape[0]), float(fps or 24.0))


# ═══════════════════════════════════════════════════════════════════════════
#  NODO 5: Video Passthrough (sin cambios, por compatibilidad)
# ═══════════════════════════════════════════════════════════════════════════

class VideoPassthroughOrSkip:
    @classmethod
    def INPUT_TYPES(cls):
        return {"required": {"video": ("VIDEO",)}}

    RETURN_TYPES = ("VIDEO",)
    RETURN_NAMES = ("video",)
    FUNCTION = "passthrough"
    CATEGORY = "Seed1.5/Flow"

    def passthrough(self, video):
        return (video,)


# ═══════════════════════════════════════════════════════════════════════════
#  Helpers internos
# ═══════════════════════════════════════════════════════════════════════════

def _get_components(video):
    if hasattr(video, "get_components"):
        try:
            return video.get_components()
        except Exception:
            return None
    return None


def _stack_frames_and_fps(videos):
    """
    Extrae frames y fps de una lista de VIDEO y los devuelve como
    (tensor IMAGE (N,H,W,C), fps). Redimensiona al tamaño del primero si hace falta.
    """
    import torch
    import torch.nn.functional as F

    all_frames = []
    fps = None

    for v in videos:
        comp = _get_components(v)
        imgs = None
        if comp is not None:
            imgs = getattr(comp, "images", None)
            if imgs is None and isinstance(comp, dict):
                imgs = comp.get("images")
            if fps is None:
                fps = getattr(comp, "fps", None) or (comp.get("fps") if isinstance(comp, dict) else None)
        if imgs is None and isinstance(v, torch.Tensor):
            imgs = v
        if imgs is None:
            raise RuntimeError(
                "VideoConcatParallel: no pude extraer frames de un VIDEO. "
                "Tu pack devuelve un formato no soportado."
            )
        all_frames.append(imgs)

    target_shape = all_frames[0].shape[1:3]
    resized = []
    for t in all_frames:
        if t.shape[1:3] != target_shape:
            x = t.permute(0, 3, 1, 2)
            x = F.interpolate(x, size=target_shape, mode="bilinear", align_corners=False)
            t = x.permute(0, 2, 3, 1)
        resized.append(t)

    return torch.cat(resized, dim=0), (fps or 24.0)


def _concat_videos(videos):
    """
    Concatena videos de forma best-effort. Si el pack expone VideoFromComponents
    lo usa; si no, devuelve el tensor de frames (ComfyUI moderno acepta
    un batch IMAGE como VIDEO en muchos contextos).
    """
    frames, fps = _stack_frames_and_fps(videos)

    try:
        from comfy_api.input_impl.video_types import VideoFromComponents  # type: ignore
        try:
            from comfy_api.input_impl.video_types import VideoComponents  # type: ignore
            comp = VideoComponents(images=frames, audio=None, fps=float(fps))
        except Exception:
            comp = type("VC", (), {"images": frames, "audio": None, "fps": float(fps)})()
        return VideoFromComponents(comp)
    except Exception:
        return frames


# ═══════════════════════════════════════════════════════════════════════════

NODE_CLASS_MAPPINGS = {
    "Seed15_DurationGate":        DurationGate,
    "Seed15_DurationGateVideo":   DurationGateVideo,
    "Seed15_VideoConcatFiltered": VideoConcatFiltered,
    "Seed15_VideoConcatParallel": VideoConcatParallel,
    "Seed15_VideoPassthrough":    VideoPassthroughOrSkip,
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "Seed15_DurationGate":        "Duration Gate (Seed 1.5)",
    "Seed15_DurationGateVideo":   "Duration Gate Video (Seed 1.5)",
    "Seed15_VideoConcatFiltered": "Video Concat Filtered (Seed 1.5)",
    "Seed15_VideoConcatParallel": "Video Concat Parallel (Seed 1.5)",
    "Seed15_VideoPassthrough":    "Video Passthrough (Seed 1.5)",
}
