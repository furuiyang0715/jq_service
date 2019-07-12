import time

import schedule

from update_canlendars.run import update


def run():
    # 生产者
    schedule.every(10).seconds.do(update)
    update()

    while True:
        # 消费者
        schedule.run_pending()
        time.sleep(2)

run()
