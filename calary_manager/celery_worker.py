from celery import Celery
import subprocess

celery_app = Celery("tasks")
celery_app.config_from_object("celeryconfig")

@celery_app.task(bind=True)
def run_external_project(self):
    project_path = "/home/maksud/Документы/ls_ml_beckend_detection/detection"
    print(f"📁 Запускаю из: {project_path}")

    try:
        result = subprocess.run(
            ["python", "main.py", "stream"],
            cwd=project_path,
            capture_output=True,
            text=True,
            check=True
        )
        print("✅ Запущено успешно:\n", result.stdout)
        if result.stderr:
            print("⚠️ stderr:\n", result.stderr)
    except subprocess.CalledProcessError as e:
        print("❌ Ошибка при запуске:\n", e.stderr)
    except Exception as e:
        print("🔥 Общая ошибка:", str(e))
