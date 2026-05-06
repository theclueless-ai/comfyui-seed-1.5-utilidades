# comfyui-seed-1.5-utilidades/audio_utils.py
#
# Audio helpers for the Seedance pipeline.
#
# AudioPadToMin: pads an AUDIO clip with trailing silence so its duration
# reaches at least `min_seconds`. Used to fix the case where AudioCrop
# slices a fixed 10s window and the last chunk is shorter than the
# Seedance minimum (4s), which otherwise causes DurationGateVideo to skip
# the segment and drop it from the final video.


class AudioPadToMin:
    """
    Pad an AUDIO clip with silence at the end until its duration is at
    least `min_seconds`. Clips already long enough pass through unchanged.

    Wiring:
        [AudioCrop] -> [AudioPadToMin] -> [AudioDuration] -> [MathExpression]
                                       -> [Apply WhisperX]

    With this node in place, the chunk fed to AudioDuration is never
    shorter than `min_seconds`, so `a * (a >= 4)` no longer zeros it out
    and DurationGateVideo lets the segment through. Seedance still
    receives the real (padded) duration as an int.
    """

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "audio": ("AUDIO",),
                "min_seconds": ("FLOAT", {
                    "default": 4.0,
                    "min": 0.0,
                    "max": 999999.0,
                    "step": 0.1,
                    "tooltip": (
                        "Duración mínima en segundos. Si el audio dura menos, "
                        "se rellena con silencio al final hasta llegar a este valor."
                    ),
                }),
            },
        }

    RETURN_TYPES = ("AUDIO",)
    RETURN_NAMES = ("audio",)
    FUNCTION = "pad"
    CATEGORY = "Seed1.5/Audio"

    def pad(self, audio, min_seconds):
        import torch

        if isinstance(audio, dict):
            wf = audio.get("waveform")
            sr = audio.get("sample_rate")
        else:
            wf = getattr(audio, "waveform", None)
            sr = getattr(audio, "sample_rate", None)

        if wf is None or sr is None or not isinstance(wf, torch.Tensor):
            print("[AudioPadToMin] formato de AUDIO no reconocido, paso passthrough.")
            return (audio,)

        sr = int(sr)
        n_samples = int(wf.shape[-1])
        cur_seconds = n_samples / float(sr) if sr > 0 else 0.0

        if cur_seconds >= float(min_seconds):
            return (audio,)

        target_samples = int(round(float(min_seconds) * sr))
        pad_samples = max(0, target_samples - n_samples)
        if pad_samples == 0:
            return (audio,)

        pad_shape = list(wf.shape)
        pad_shape[-1] = pad_samples
        silence = torch.zeros(pad_shape, dtype=wf.dtype, device=wf.device)
        new_wf = torch.cat([wf, silence], dim=-1)

        print(
            f"[AudioPadToMin] padded {cur_seconds:.2f}s -> "
            f"{target_samples / sr:.2f}s (+{pad_samples} samples)."
        )
        return ({"waveform": new_wf, "sample_rate": sr},)


NODE_CLASS_MAPPINGS = {
    "Seed15_AudioPadToMin": AudioPadToMin,
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "Seed15_AudioPadToMin": "Audio Pad To Min (Seed 1.5)",
}
