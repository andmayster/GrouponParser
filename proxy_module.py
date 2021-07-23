import os
import json
import sys
from time import time, sleep
from random import randint


stop = open('stop.txt', 'w+')
stop.close()


def stop_check():
    if not os.path.exists('stop.txt'):
        print('file deleted<br>')
        print('<h2>Script stopped</h2>')
        sys.exit(0)


def create_proxy_list():

    proxies_info = []

    proxy_list = '/home/firstbank/domains/firstbankmi.com/public_html/mcfreyze/proxy/proxies.txt'
    # wrong_proxy_list = '/home/firstbank/domains/firstbankmi.com/public_html/andrew_workspace/parsers/nextDoor/test_proxy.txt'
    with open(proxy_list, 'r', encoding='UTF-8') as all_proxies:
        for line in all_proxies:
            stop_check()
            ip = line.split('|')[0]
            user_info = line.split('|')[1]
            username = user_info.split(':')[0]
            password = user_info.split(':')[1]
            ban_time = line.split('|')[3]

            proxy_info = {
                "ip": ip,
                'username': username,
                'password': password,
                'ban_time': int(ban_time)
            }
            proxies_info.append(proxy_info)
    return proxies_info


def create_proxy_local_file(log_name='', proxies_info=create_proxy_list()):
    with open(f'logs_{log_name}_proxy.json', 'w+') as local_proxy:
        local_proxy.write(f'{json.dumps(proxies_info, indent=4)}')


def get_not_banned_proxy(proxies_info, time_to_be_banned=600, time_to_sleep=300):
    proxy_check_counter = 0

    while True:
        stop_check()

        random_proxy_index = randint(0, len(proxies_info) - 1)
        random_proxy = proxies_info[random_proxy_index]

        if proxy_check_counter >= 1000:
            sleep(time_to_sleep)
            proxy_check_counter = 0
            continue

        is_proxy_banned = round(time()) - int(random_proxy['ban_time']) < time_to_be_banned

        if is_proxy_banned:
            proxy_check_counter += 1
            random_proxy_index = randint(0, len(proxies_info) - 1)
            random_proxy = proxies_info[random_proxy_index]
            continue
        else:
            break

    return random_proxy, random_proxy_index


def ban_proxy(proxies_info, random_proxy_index, log_name):
    proxies_info[random_proxy_index]['ban_time'] = round(time())
    create_proxy_local_file(log_name, proxies_info)



