import json
from redis.asyncio import Redis
r = Redis(host='localhost', port=6379, db=0)

async def remove_task_from_redis(task_id: str):
    redis_key = f"payload:{task_id}"
    all_items = await r.lrange(redis_key, 0, -1)
    to_remove = None
    for item in all_items:
        try:
            item_data = json.loads(item)
            if item_data.get("data", {}).get("task_id") == task_id:
                to_remove = item
                break
        except Exception:
            continue

    if to_remove:
        await r.lrem(redis_key, 0, to_remove)
        return f"✅ task_id {task_id} удалён из {redis_key}"
    else:
        return f"⚠️ task_id {task_id} не найден в {redis_key}"

