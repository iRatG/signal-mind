from PIL import Image
import os

INPUT  = "habr/sample/charts/obsidian.gif"
OUTPUT = "habr/sample/charts/obsidian_opt.gif"
MAX_WIDTH = 900
KEEP_EVERY = 2  # оставить каждый N-й кадр

src = Image.open(INPUT)
scale = MAX_WIDTH / src.size[0]
new_size = (MAX_WIDTH, int(src.size[1] * scale))

frames = []
durations = []

i = 0
while True:
    try:
        src.seek(i)
    except EOFError:
        break
    if i % KEEP_EVERY == 0:
        frame = src.convert("RGBA").resize(new_size, Image.LANCZOS)
        frame = frame.convert("P", palette=Image.ADAPTIVE, colors=128)
        frames.append(frame)
        dur = src.info.get("duration", 80)
        durations.append(dur * KEEP_EVERY)
    i += 1

print(f"Кадров после прореживания: {len(frames)} (было {i})")
print(f"Новый размер: {new_size}")

frames[0].save(
    OUTPUT,
    save_all=True,
    append_images=frames[1:],
    optimize=True,
    loop=0,
    duration=durations,
)

size_before = os.path.getsize(INPUT) / 1024 / 1024
size_after  = os.path.getsize(OUTPUT) / 1024 / 1024
print(f"До:    {size_before:.1f} MB")
print(f"После: {size_after:.1f} MB  ({100 * size_after / size_before:.0f}%)")
