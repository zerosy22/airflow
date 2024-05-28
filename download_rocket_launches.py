# Airflow DAG 작성 첫 실습입니다.

# json 파싱을 위한 라이브러리
import json
# 이미지 파일을 다운로드 받아서 저장할 때 경로 설정을 위한 라이브러리
import pathlib

# 이미지 다운로드 받기 위한 라이브러리
import requests
import requests.exceptions as requests_exceptions

# 에어플로우 관련 라이브러리
import airflow.utils.dates
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# 옵션 설정
dag = DAG(
    dag_id="download_rocket_launches",  # 보통 파일명과 똑같이
    description="Image Download",
    start_date=airflow.utils.dates.days_ago(14),  # 14일 전으로 설정할수도 있음
    schedule_interval=None
)

# 리눅스 배시 명령 수행
download_launches = BashOperator(
    tast_id="download_launches",
    bash_command="curl -o /tmp/launches.json -L https://ll.thespacedevs.com/2.0.0/launch/upcoming/",  # noqa: E501
    dag=dag,  # 대그이름
)

# 파이썬 명령
def _get_pictures():
    pathlib.Path("/tmp/images").mkdir(parents=True, exist_ok=True)

    with open("/tmp/launches.json") as f:
        # JSON 파싱
        launches = json.load(f)

        # image url 가져오기
        image_urls = [launch["images"] for launch in launches["results"]]

        # 순회하면서 다운로드
        for image_url in image_urls:
            try:
                # 다운로드
                response = requests.get(image_url)
                # 파일명을 만들기 위해서 마지막 / 다음의 문자열 추출
                image_filename = image_url.split("/")[-1]
                # 이미지 저장 경로 생성
                target_file = f"/tmp/images/{image_filename}"
                with open(target_file, "wb") as f:
                    f.write(response.content)
                print(f"Downloaded {image_url} to {target_file}")

            except requests_exceptions.MissingSchema:
                print(f"{image_url} appears to be an invalid URL.")
            except requests_exceptions.ConnectionError:
                print(f"Could not connect to {image_url}.")

            # except:
            #     print("image download fail")

# DAG 에서 Python 함수를 호출
get_pictures = PythonOperator(
    task_id="get_pictures",
    python_callable=_get_pictures,
    dag=dag,
)

# 알림 설정
notify = BashOperator(
    task_id="notify",
    bash_command='echo "There are now $(ls /tmp/images/ | wc -l) images."',
    dag=dag,
)

# 태스크 실행 순서 설정
download_launches >> get_pictures >> notify

