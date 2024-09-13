import os
import ray
import time
import pandas as pd
from paddleocr import PaddleOCR

if not ray.is_initialized():
    ray.init()

start_time = time.time()
start_formatted_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start_time))
print(f"Start time: {start_formatted_time}")


@ray.remote
class StructureActor:
    def __init__(self):
        self.df=pd.DataFrame(columns=["file_path", "group_id", "entity_name", "entity_value","entity_text"])

    def update_df(self, new_row):
        self.df = pd.concat([self.df, pd.DataFrame([new_row])], ignore_index=True)

    def get_df(self):
        return self.df


@ray.remote
def PaddleOCRLocal(row):
    try:
        file_path=row['image_path']
        group_id=row['group_id']
        entity_name=row['entity_name']
        entity_value=row['entity_value']
        print(entity_value)

        label = None
    
        ocr_model = PaddleOCR(use_angle_cls=True, lang='en', show_log=False)
        text_result = ocr_model.ocr(file_path)

        final_text=""

        for item in text_result:
            for sub_item in item:
                label = sub_item[1][0]
                final_text+=" "+label

        final={
            "file_path":file_path,
            "group_id": group_id,
            "entity_name": entity_name,
            "entity_value": entity_value,
            "entity_text":str(final_text)
        }

        return final

    except Exception as e:
        final={
            "file_path":"",
            "group_id": "",
            "entity_name": "",
            "entity_value": "",
            "entity_text":""
        }
        print("Exception:", e)
        return final

batch_size = 20
df=pd.read_csv('img_path_dataset.csv')
structure_actor=StructureActor.remote()

for i in range(0, df.shape[0], batch_size):
    new_df=df.iloc[i:i + batch_size]
    futures = [PaddleOCRLocal.remote(row) for _, row in new_df.iterrows()]
    results=ray.get(futures)
    for result in results:
        structure_actor.update_df.remote(result)

end_time=time.time()
end_formatted_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end_time))
print(f"End time: {end_formatted_time}")

final_df=ray.get(structure_actor.get_df.remote())
final_df.to_csv("final_df.csv", index=False)