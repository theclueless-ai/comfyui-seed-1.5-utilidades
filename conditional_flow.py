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

    PRUEBA

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

    RETURN_TYPES = ("VIDEO", "IMAGE", "AUDIO", "INT", "FLOAT")
    RETURN_NAMES = ("video", "frames", "audio", "frame_count", "fps")
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

        frames, fps, audio = _stack_frames_fps_and_audio(videos)
        video_out = _concat_videos(videos, frames, fps, audio)
        return (video_out, frames, audio, int(frames.shape[0]), float(fps or 24.0))


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


def _comp_attr(comp, *names, default=None):
    """Lee el primer atributo disponible (o clave si comp es dict)."""
    for name in names:
        if hasattr(comp, name):
            v = getattr(comp, name)
            if v is not None:
                return v
        if isinstance(comp, dict) and name in comp and comp[name] is not None:
            return comp[name]
    return default


def _stack_frames_fps_and_audio(videos):
    """
    Extrae frames, frame_rate y audio de una lista de VIDEO y los devuelve como
    (tensor IMAGE (N,H,W,C), frame_rate, audio_dict_o_None).

    - Frames: redimensiona al tamaño del primero si hace falta y concatena.
    - Audio: concatena waveforms en el eje de samples. Si un clip no trae
      audio se inserta silencio por la duración de sus frames para mantener
      la sincronía con el video. Si ninguno trae audio, devuelve None.
    """
    import torch
    import torch.nn.functional as F

    all_frames = []
    audio_chunks = []   # lista de (waveform (C, samples), sample_rate, n_frames)
    frame_rate = None

    for v in videos:
        comp = _get_components(v)
        imgs = None
        audio = None
        if comp is not None:
            imgs = _comp_attr(comp, "images")
            audio = _comp_attr(comp, "audio")
            if frame_rate is None:
                frame_rate = _comp_attr(comp, "frame_rate", "fps")
        if imgs is None and isinstance(v, torch.Tensor):
            imgs = v
        if imgs is None:
            raise RuntimeError(
                "VideoConcatParallel: no pude extraer frames de un VIDEO. "
                "Tu pack devuelve un formato no soportado."
            )
        all_frames.append(imgs)
        audio_chunks.append((audio, imgs.shape[0]))

    fps = float(frame_rate or 24.0)

    target_shape = all_frames[0].shape[1:3]
    resized = []
    for t in all_frames:
        if t.shape[1:3] != target_shape:
            x = t.permute(0, 3, 1, 2)
            x = F.interpolate(x, size=target_shape, mode="bilinear", align_corners=False)
            t = x.permute(0, 2, 3, 1)
        resized.append(t)
    frames_out = torch.cat(resized, dim=0)

    audio_out = _concat_audio_chunks(audio_chunks, fps)
    return frames_out, fps, audio_out


def _normalize_audio(audio):
    """
    Devuelve (waveform_2d (C, samples), sample_rate) a partir del tipo AUDIO
    de ComfyUI — `{"waveform": Tensor(B, C, S) o (C, S), "sample_rate": int}` —
    o de un objeto similar. None si no hay audio.
    """
    import torch
    if audio is None:
        return None
    wf = None
    sr = None
    if isinstance(audio, dict):
        wf = audio.get("waveform")
        sr = audio.get("sample_rate")
    else:
        wf = getattr(audio, "waveform", None)
        sr = getattr(audio, "sample_rate", None)
    if wf is None or sr is None:
        return None
    if not isinstance(wf, torch.Tensor):
        return None
    # Normalize shape to (C, S)
    if wf.dim() == 3:
        wf = wf[0]
    elif wf.dim() == 1:
        wf = wf.unsqueeze(0)
    return wf, int(sr)


def _concat_audio_chunks(audio_chunks, fps):
    """
    audio_chunks: list of (audio, n_frames). Concatena en el eje de samples;
    rellena con silencio los clips sin audio para mantener sincronía.
    Devuelve dict `{"waveform": Tensor(1, C, S), "sample_rate": int}` o None
    si ningún clip tiene audio.
    """
    import torch

    normalized = [(_normalize_audio(a), nf) for a, nf in audio_chunks]
    real = [(na, nf) for na, nf in normalized if na is not None]
    if not real:
        return None

    # Tomar sample_rate y channels del primer audio real
    ref_sr = real[0][0][1]
    ref_ch = real[0][0][0].shape[0]

    parts = []
    for na, n_frames in normalized:
        if na is None:
            silence_samples = int(round((n_frames / fps) * ref_sr))
            parts.append(torch.zeros((ref_ch, silence_samples),
                                     dtype=real[0][0][0].dtype,
                                     device=real[0][0][0].device))
            continue
        wf, sr = na
        if sr != ref_sr:
            # Resample simple: si no coincide sr, rellenamos con silencio
            # (resamplear con torch sin torchaudio es caro; evitar aquí).
            silence_samples = int(round((n_frames / fps) * ref_sr))
            parts.append(torch.zeros((ref_ch, silence_samples),
                                     dtype=wf.dtype, device=wf.device))
            print(f"[VideoConcatParallel] audio SR {sr}≠{ref_sr}, reemplazado por silencio.")
            continue
        if wf.shape[0] != ref_ch:
            if wf.shape[0] == 1 and ref_ch > 1:
                wf = wf.repeat(ref_ch, 1)
            elif wf.shape[0] > ref_ch:
                wf = wf[:ref_ch]
            else:
                pad = torch.zeros((ref_ch - wf.shape[0], wf.shape[1]),
                                  dtype=wf.dtype, device=wf.device)
                wf = torch.cat([wf, pad], dim=0)
        parts.append(wf)

    waveform = torch.cat(parts, dim=1).unsqueeze(0)  # (1, C, S)
    return {"waveform": waveform, "sample_rate": ref_sr}


def _load_video_types():
    """Intenta importar (VideoFromComponents, VideoComponents) de varias rutas."""
    candidates = (
        "comfy_api.latest._input_impl.video_types",
        "comfy_api.input_impl.video_types",
    )
    import importlib
    for path in candidates:
        try:
            mod = importlib.import_module(path)
            vfc = getattr(mod, "VideoFromComponents", None)
            vc  = getattr(mod, "VideoComponents",   None)
            if vfc is not None:
                return vfc, vc
        except Exception:
            continue
    return None, None


def _concat_videos(videos, frames=None, frame_rate=None, audio=None):
    """
    Construye un VIDEO a partir de los frames/fps/audio concatenados.
    Si no se pasaron, los extrae de la lista de videos.
    """
    if frames is None or frame_rate is None:
        frames, frame_rate, audio = _stack_frames_fps_and_audio(videos)

    VideoFromComponents, VideoComponents = _load_video_types()
    if VideoFromComponents is None:
        return frames

    comp = None
    if VideoComponents is not None:
        for kwargs in (
            {"images": frames, "audio": audio, "frame_rate": frame_rate},
            {"images": frames, "audio": audio, "fps": frame_rate},
        ):
            try:
                comp = VideoComponents(**kwargs)
                break
            except Exception:
                continue
    if comp is None:
        comp = type("VC", (), {
            "images":     frames,
            "audio":      audio,
            "frame_rate": frame_rate,
            "fps":        frame_rate,
        })()

    try:
        return VideoFromComponents(comp)
    except Exception as exc:
        print(f"[VideoConcatParallel] VideoFromComponents falló ({exc}); devuelvo frames.")
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
