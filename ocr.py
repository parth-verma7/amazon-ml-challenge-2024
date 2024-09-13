import os
import ray
import time
from paddleocr import PaddleOCR

if not ray.is_initialized():
    ray.init()

files=[]
for filename in os.listdir("./images"):
    file_path = os.path.join("./images", filename)
    files.append(file_path)

start_time = time.time()
formatted_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start_time))
print(f"Start time: {formatted_time}")

@ray.remote
def PaddleOCRLocal(file_path):
    label = None
    ocr_model = PaddleOCR(use_angle_cls=True, lang='en', show_log=False)
    text_result = ocr_model.ocr(file_path)

    final=""

    for item in text_result:
        for sub_item in item:
            label = sub_item[1][0]
            final+=" "+label

    return str(final)

batch_size = 20
for i in range(0, len(files), batch_size):
    batch_files = files[i:i + batch_size]
    futures = [PaddleOCRLocal.remote(filepath) for filepath in batch_files]
    results=ray.get(futures)
    print(results)

end_time=time.time()
formatted_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end_time))
print(f"Start time: {formatted_time}")