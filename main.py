import requests
import re
import json
import cloudscraper
import datetime

url = 'https://www.groupon.com/coupons/dollar-general'

scraper = cloudscraper.create_scraper()

html_doc = scraper.get(url, timeout=10)

print(html_doc.status_code)

# # https://regex101.com/r/WIQpvH/1/
# regex = r"\"coupons\":(\[.*?\"VC_MerchantUserOfferWithBranch\"}])}"
# match = re.search(regex, html_doc.text)
#
# if match is not None:
#     coupons = match.group(1)
#
#     coupons = json.loads(coupons)
#
#     for coupon in coupons:
#         print(coupon)
#         print(f"Name coupon -{coupon['offerTitle']}")
#         print(f"Description coupon - {coupon['offerDescription']}")
#         offer_id = coupon['offerId']
#         print(f'Offer id - {offer_id}')
#         offer_type = coupon['offerType']
#         url_code = 'https://www.groupon.com/coupons/redemption/' + str(offer_id)
#         second_get = scraper.get(url_code, timeout=10)
#         var = second_get.json()
#         print(var)
#         offer_code = var['offerCode']
#         merchantId = var['merchantId']
#         print(f'OFFER CODE - {offer_code}')
#         get = scraper.get('https://www.groupon.com/coupons/redirect/merchant/' + str(merchantId))
#         url = get.request.url
#         print(f'Url - {url}')

# https://regex101.com/r/WIQpvH/2
regex = r"\"coupons\":(\[.*?\"VC_MerchantUserOfferWithBranch\"}])"
match = re.search(regex, html_doc.text)

coupons = []

# count = 0
if match is not None:
    coupons_jsn = match.group(1)

    coupons_jsn = json.loads(coupons_jsn)

    for coupon in coupons_jsn:
        # if count == 5: # временно, пока тестируем через веб
        #     break
        coup = dict()
        try:
            coup['title'] = coupon['offerTitle']

            # daily_test
            get_coupon_name = True

            if coup['title'] is None:
                coup['title'] = 'None'
        except:
            coup['title'] = 'None'
        try:
            coup['description'] = coupon['offerDescription']

            # daily_test
            get_coupon_description = True

            if coup['description'] is None:
                coup['description'] = 'None'
        except:
            coup['description'] = 'None'

        offer_id = coupon['offerId']

        # offer_type = coupon['offerType']
        try:
            url_code = 'https://www.groupon.com/coupons/redemption/' + str(offer_id)
            second_get = scraper.get(url_code, timeout=10)
            var = second_get.json()
            offer_code = var['offerCode']
            coup['code'] = offer_code

            # daily_test
            get_coupon_code = True

            if coup['code'] is None:
                coup['code'] = 'None'
        except:
            coup['code'] = 'None'

        try:
            merchantId = var['merchantId']
            third_get = scraper.get('https://www.groupon.com/coupons/redirect/merchant/' + str(merchantId), timeout=10)
            coup['url'] = third_get.request.url

            get_coupon_url = True

            if coup['url'] is None:
                coup['url'] = 'None'
        except:
            coup['url'] = 'None'

        try:
            date = var['expiryDateTime']

            if date is None:
                coup['expiration_date'] = 'None'
            elif date == 'Null':
                coup['expiration_date'] = 'None'

            elif date is not None:

                date = date.split('.')[0]
                coup['expiration_date'] = datetime.strptime(date, "%Y-%m-%dT%H:%M:%S").timestamp()

                # daily_test
                get_coupon_expiration_date = True

        except:
            coup['expiration_date'] = 'None'

        # ищем в названии
        if '$' in str(coup['title']):
            coup['type'] = '$'
        elif '%' in str(coup['title']):
            coup['type'] = '%'
        elif 'ship' in str(coup['title']) or 'shipping' in str(coup['title']):
            coup['type'] = 'FS'
        else:
            coup['type'] = 'DEAL'

        if coup['type'] == 'DEAL':
            # ищем в описании
            if '$' in str(coup['description']):
                coup['type'] = '$'
            elif '%' in str(coup['description']):
                coup['type'] = '%'
            elif 'ship' in str(coup['description']) or 'shipping' in str(coup['description']):
                coup['type'] = 'FS'
            else:
                coup['type'] = 'DEAL'

        # https://regex101.com/r/5NQHCp/1
        if coup['type'] == '$':
            regex = r"\$([0-9,.]+)"  # ищем любое число(первое вхождение)
            match = re.search(regex, coup['title'])
            coup['amount'] = ''
            if match is not None:
                coup['amount'] = match.group(1)
            elif match is None:
                match = re.search(regex, coup['description'])

                if match is not None:
                    coup['amount'] = match.group(1)
                else:
                    coup['amoumt'] = ''

            else:
                coup['amount'] = ''

        elif coup['type'] == '%':
            regex = r"([0-9,.]+%)"  # ищем любое число(первое вхождение)
            match = re.search(regex, coup['title'])
            coup['amount'] = ''
            if match is not None:
                coup['amount'] = match.group(1)
            elif match is None:
                match = re.search(regex, coup['description'])

                if match is not None:
                    coup['amount'] = match.group(1)
                else:
                    coup['amoumt'] = ''

            else:
                coup['amount'] = ''

        else:
            coup['amount'] = ''

        coupons.append(coup)
        # count += 1

# if coupons:
#     for c in coupons:
#         if print_on_page:
#             print("<b>COUPON</b>")
#             print(f'<br>Name coupon - {c["title"].encode("utf-8")}<br>')
#             print(f'<br>Description coupon - {c["description"].encode("utf-8")}<br>')
#             print(f'<br>Code coupon - {c["code"]}<br>')
#             print(f'<br>Url coupon - {c["url"]}<br>')
#             print(f'<br>Expiration date coupon - {c["expiration_date"]}<br>')
#             print("<br><hr>")

if coupons:
    for c in coupons:
        c['title'] = c['title'].replace('None', '')
        c['description'] = c['description'].replace('None', '')
        c['code'] = c['code'].replace('None', '')
        c['url'] = c['url'].replace('None', '')
        # if not c['expiration_date'].isdigit():
        if 'None' in str(c['expiration_date']):
            c['expiration_date'] = str(c['expiration_date']).replace('None', '')

        if c['amount'] == ',' or c['amount'] == '.':
            c['amount'] = c['amount'].replace(',', '').replace('.', '')

        c['amount'] = c['amount'].replace('2021', '').replace('2022', '').replace('2019', '').replace('2020', '')

        if c['amount'] != '':
            if c['amount'][-1] == '.' or c['amount'][-1] == ',':
                c['amount'] = c['amount'][:-1]


if coupons:
    for c in coupons:
        # if print_on_page:
        print("<b>COUPON</b>")
        print(f'<br>Name coupon - {c["title"].encode("utf-8")}<br>')
        print(f'<br>Description coupon - {c["description"].encode("utf-8")}<br>')
        print(f'<br>Code coupon - {c["code"]}<br>')
        print(f'<br>Url coupon - {c["url"]}<br>')
        print(f'<br>Expiration date coupon - {c["expiration_date"]}<br>')
        print(f'<br>Type coupon - {c["type"]}<br>')
        print(f'<br>Amount coupon - {c["amount"]}<br>')

