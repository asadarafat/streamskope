#!/usr/bin/env python3
"""Original 36-second / 120 BPM score. Stdlib synthesis; FFmpeg AAC mastering.

Run from any directory: python3 website/promo/create-score.py
No samples, external music, or additional Python packages. Apache-2.0.
"""
import array
import json
import math
from pathlib import Path
import random
import re
import subprocess

RATE, SECONDS, BEAT = 48000, 36, 0.5
TAU = math.tau
OUT = Path(__file__).resolve().parents[2] / '.artifacts/website/energetic-score'
OUT.mkdir(parents=True, exist_ok=True)
left = array.array('f', [0]) * (RATE * SECONDS)
right = array.array('f', [0]) * (RATE * SECONDS)
rng = random.Random(1042)


def hz(note):
    return 440 * 2 ** ((note - 69) / 12)


def voice(start, duration, amplitude, kind, note=50, pan=0):
    """Short band-limited tonal voices and filtered, seeded noise percussion."""
    offset = round(start * RATE)
    count = min(round(duration * RATE), len(left) - offset)
    frequency, filtered, previous = hz(note), 0, 0
    lg, rg = math.sqrt((1 - pan) / 2), math.sqrt((1 + pan) / 2)
    for i in range(count):
        t = i / RATE
        attack = min(1, t / 0.005)
        release = min(1, (count - i) / (RATE * 0.035))
        if kind == 'kick':
            phase = TAU * (47 * t + 1.15 * (1 - math.exp(-t * 38)))
            value = math.sin(phase) * math.exp(-t * 13)
            value += 0.055 * rng.uniform(-1, 1) * math.exp(-t * 180)
        elif kind in ('hat', 'snare', 'riser'):
            noise = rng.uniform(-1, 1)
            filtered += 0.24 * (noise - filtered)
            high = noise - filtered
            if kind == 'hat':
                value = high * math.exp(-t * 48)
            elif kind == 'snare':
                value = (0.8 * high + 0.2 * filtered) * math.exp(-t * 21)
                value += 0.27 * math.sin(TAU * 185 * t) * math.exp(-t * 35)
            else:
                previous += 0.07 * (noise - previous)
                value = previous * (t / duration) ** 2 * (0.7 + 0.3 * math.sin(TAU * 8 * t))
        elif kind == 'bass':
            phase = TAU * frequency * t
            value = (math.sin(phase) + 0.23 * math.sin(2 * phase) + 0.07 * math.sin(3 * phase))
            value *= math.exp(-t * 6.5)
        elif kind == 'pluck':
            phase = TAU * frequency * t
            value = math.sin(phase) * math.exp(-t * 5.5)
            value += 0.32 * math.sin(phase * 2.002) * math.exp(-t * 12)
            value += 0.13 * math.sin(phase * 3) * math.exp(-t * 19)
        else:  # Slowly opening stereo harmony, gently ducked under each kick.
            phase = TAU * frequency * t
            value = (math.sin(phase) + 0.35 * math.sin(phase * 1.002) + 0.14 * math.sin(phase * 2))
            value *= min(1, t / 0.22) * min(1, (duration - t) / 0.6)
            value *= 0.62 + 0.38 * min(1, ((start + t) % BEAT) / 0.18)
        value *= amplitude * attack * release
        left[offset + i] += value * lg
        right[offset + i] += value * rg


# D major / B minor color; final D(add9) provides an unambiguous warm resolution.
chords = [(50, 57, 61, 66), (47, 54, 59, 62), (43, 50, 57, 59), (45, 52, 59, 61)]
for bar in range(16):
    start = bar * 2
    chord = chords[(bar // 2) % 4]
    for index, pitch in enumerate(chord):
        voice(start, 2.4, 0.047 if start >= 6 else 0.032, 'pad', pitch, (-0.6, 0.6, -0.3, 0.3)[index])
    # Four-note motif changes register and rhythm instead of looping one beep.
    for j, beat in enumerate((0, 0.75, 1.5, 2.5, 3.25)):
        moment = start + beat * BEAT
        pitch = chord[(j + bar) % 4] + (24 if bar % 4 == 3 else 12)
        gain = 0.085 if moment >= 6 else 0.055
        if moment >= 30:
            gain *= 0.55
        voice(moment, 0.65, gain, 'pluck', pitch, (-0.35, 0.25)[j % 2])
        voice(moment + 0.375, 0.58, gain * 0.22, 'pluck', pitch, (0.5, -0.5)[j % 2])
    if start >= 4:
        for beat in (0, 0.75, 1.5, 2, 2.75, 3.5):
            if start >= 30 and beat > 1:
                continue
            voice(start + beat * BEAT, 0.28, 0.29 if start >= 6 else 0.18, 'bass', chord[0] - 12)

for beat in range(64):
    t = beat * BEAT
    if t < 2 or (t < 6 and beat % 2) or t >= 30.5:
        continue
    voice(t, 0.48, 0.62, 'kick')
    if beat % 2 and t >= 6:
        voice(t, 0.23, 0.19, 'snare', pan=0.05)
    voice(t + 0.25, 0.13, 0.105, 'hat', pan=(-0.3, 0.3)[beat % 2])
    if 12 <= t < 30:
        voice(t + 0.125, 0.075, 0.039, 'hat', pan=-0.5)
        voice(t + 0.375, 0.075, 0.045, 'hat', pan=0.5)

# Edit anchors: workflow reveal, consumer result, montage, return, brand.
for transition in (6, 19, 24, 30, 32):
    voice(transition - 0.8, 0.8, 0.17, 'riser', pan=0.2)
    voice(transition, 0.55, 0.32, 'kick')
    for pitch in (74, 81, 85):
        voice(transition, 1.0, 0.045, 'pluck', pitch, (pitch - 80) / 20)
for t in (23.5, 23.75, 23.875, 29.5, 29.75):
    voice(t, 0.16, 0.1, 'snare', pan=-0.1)
for index, pitch in enumerate((50, 57, 62, 66, 76)):
    voice(32, 3.8, 0.082, 'pad', pitch, (index - 2) / 4)
for t, pitch in ((32, 74), (32.25, 78), (32.5, 81), (33, 86)):
    voice(t, 2.0, 0.1, 'pluck', pitch, 0.2)
voice(32, 0.5, 0.28, 'bass', 38)

raw = OUT / 'energetic-score.f32le'
interleaved = array.array('f')
for i, (l, r) in enumerate(zip(left, right)):
    fade = min(1, i / (RATE * 0.015), (len(left) - i) / (RATE * 0.35))
    interleaved.extend((l * fade, r * fade))
with raw.open('wb') as handle:
    interleaved.tofile(handle)
source = ['-f', 'f32le', '-ar', str(RATE), '-ac', '2', '-i', str(raw)]
first = subprocess.run(['ffmpeg', '-hide_banner', *source, '-af', 'loudnorm=I=-16:TP=-1.8:LRA=7:print_format=json', '-f', 'null', '-'], capture_output=True, text=True, check=True)
measured = json.loads(re.findall(r'\{[^{}]+\}', first.stderr)[-1])
normalization = ('loudnorm=I=-16:TP=-1.8:LRA=7:linear=true:'
                 f"measured_I={measured['input_i']}:measured_TP={measured['input_tp']}:"
                 f"measured_LRA={measured['input_lra']}:measured_thresh={measured['input_thresh']}:"
                 f"offset={measured['target_offset']}")
output = OUT / 'energetic-score.m4a'
subprocess.run(['ffmpeg', '-v', 'error', '-y', *source, '-af', normalization, '-ar', str(RATE), '-c:a', 'aac', '-b:a', '256k', '-t', str(SECONDS), '-movflags', '+faststart', str(output)], check=True)
probe = subprocess.run(['ffprobe', '-v', 'error', '-show_format', '-show_streams', '-of', 'json', str(output)], capture_output=True, text=True, check=True)
(OUT / 'ffprobe.json').write_text(probe.stdout)
validation = subprocess.run(['ffmpeg', '-hide_banner', '-i', str(output), '-af', 'loudnorm=I=-16:TP=-1.5:LRA=7:print_format=json', '-f', 'null', '-'], capture_output=True, text=True, check=True)
actual = json.loads(re.findall(r'\{[^{}]+\}', validation.stderr)[-1])
(OUT / 'loudness.json').write_text(json.dumps(actual, indent=2) + '\n')
assert float(actual['input_tp']) <= -1.5, actual
assert abs(float(actual['input_i']) + 16) <= 1, actual
raw.unlink()
print(json.dumps({'output': str(output), 'decoded_loudness': actual}, indent=2))
