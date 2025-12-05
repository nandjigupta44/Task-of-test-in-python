import requests
from pymongo import MongoClient
from db_config import pl_table, pdp_table
import json
import csv


def fetch_data(product_id,url,count):
    cookies = {
        '_ALGOLIA': 'anonymous-46f23d88-cac9-4bf5-93a7-c549d3331301',
        'mt.v': '2.105080933.1764748038763',
        'ftr_ncd': '6',
        'utag_main_v_id': '019ae32e2c3c00902b12a3ce34400506f002106700978',
        'utag_main_vapi_domain': 'sunglasshut.com',
        's_fid': '0B3C78C6766F992F-0E24EE9853C27148',
        'CONSENTMGR': 'consent:true%7Cts:1764748427868',
        '_gcl_au': '1.1.1231686658.1764748431',
        '_ga': 'GA1.1.1228085581.1764748432',
        'cjConsent': 'MHxOfDB8Tnww',
        'cjUser': 'ea243434-6029-4f17-a6ad-49e8e50d3222',
        '_scid': 'vlcnEDQ1MQje2bdZEMfrsARs45Ikp5Dd',
        '_fbp': 'fb.1.1764748436637.468890404817721063',
        '_pin_unauth': 'dWlkPVlUVmhOamN6WXpndE1Ua3pOUzAwT1RrekxUbGpNR0l0TW1KaE5UVTVZalkzTW1abQ',
        '_ScCbts': '%5B%5D',
        '_sctr': '1%7C1764700200000',
        'SGPF': '3DEh7Jdcw0B-QxFAgiZcZcPsaC1fQza1vLGTR5eIma7LE5gw5taGiaQ',
        'sgh-desktop-facet-state-search': '',
        'tealium_data_tags_adobeAnalytics_trafficSourceMid_thisSession': 'direct',
        'tiktok_click_id': 'undefined',
        's_cc': 'true',
        'TrafficSource_Override': '1',
        '__idcontext': 'eyJjb29raWVJRCI6IjM2S2VFVW96akRtWURnSXdRRHV3WkFPNERGeSIsImRldmljZUlEIjoiMzZLZUVUcUJ5aUU0dm1jcmo2MEpYakhCV2xnIiwiaXYiOiIiLCJ2IjoiIn0%3D',
        '_tt_enable_cookie': '1',
        '_ttp': '01KBHXWBG5F7M4SXBQAZGTK7TY_.tt.1',
        'ttcsid_CBL7BLBC77UFMFRPUAB0': '1764823945431::usq109J82McdLW1raOz_.2.1764825378793.1',
        'ttcsid': '1764823945432::8LvwpSAvBNGaocrYk7ki.2.1764825378796.0',
        'Lda_aKUr6BGRn': 'duertry.com/r/v2?',
        'Lda_aKUr6BGRr': '0',
        'WC_PERSISTENT': 'Kq6qxAn4QTdvY5xjl0E8FU9c9Bm3UT4HlMKewR9U%2Ffk%3D%3B2025-12-04+09%3A36%3A44.629_1764841004629-399951_0',
        '_cs_c': '1',
        '_cs_id': 'f7350921-c2c1-a137-bec3-8a8e18bc601d.1764841009.1.1764841009.1764841009.1.1799005009122.1.x',
        '__pdst': 'c5921377b4b14c938def6defe98433ff',
        '__rtbh.lid': '%7B%22eventType%22%3A%22lid%22%2C%22id%22%3A%224vf477ahI0RZOpGOq9zA%22%2C%22expiryDate%22%3A%222026-12-04T09%3A36%3A52.290Z%22%7D',
        'sm_uuid': '1764841577955',
        'Fm_kZf8ZQvmX': '1',
        'Ac_aqK8DtrDS': '7',
        'aka-zp': '',
        'bm_ss': 'ab8e18ef4e',
        'ak_bmsc': 'B78A67C2091C7E5A2B3159531B5A77F9~000000000000000000000000000000~YAAQnowsMcqDlauaAQAA1fIn6h6vJgjRlXMjKaubBLgZzuFspjn+Km/OdTscSf7my0+O7/Eswej1oT9kC1McGV3zD8CXiAzAinDSl7xDbD0bN14X4IYc+sJ9PB0H3Uuqtgy8a7Lx2rzWTfNW8eNq9jYx28t5W5ahLZ5huimBlnmvu0mLm07I5p/yKrlm6Iotp0phc5+gautGyqAyOD02HE8GMQMxQUwKkJRttl0WQE4Dj4gnzh0qg8CcfAZXpYOWslBiXlUGollEEwR0qhOS6zSP3cdZ8zU9MgWr2MP7sZ3Glkvdp4W3HkalDkmFlmWRFI58tp8wZJMQ6xqihQw0a/z8rrWEOxTqlki+RV8am3271g5Wq9FeYVrtrwkj1JZvK4iZcgmZY1ospJeqVzPyl1U9RPNChyt/iizBOqOY+EM+9HS+Rgw=',
        '_abck': '26A3EEEA561EE583E6B9E9BEB82609A5~0~YAAQnowsMfqDlauaAQAAAfgn6g89vwPf8HmgYaQEWNz4xFYb1FYLZjkyqOlsCUVW3eHuKbSpX1vbq6ETY0jgmNls+hlzdwip5Z/LYyZn2MbKKmM9QEo4cnqSEkxccdoyEbx3ZNrn6hzMnWlQqFP+TRd2AIeU4PrzYrN/W1QfhFWY6ehQ9JzFqdfZZR2l3pdINf1BHxwib3rSDdIaQKax6C9/EKY9sAbmGJ51BX8tjdh0PxR7W4fBZZVJvtCXLIN13bMNYGbmsJf77o1zDAbrmy+Owzg1YbPt4NcuO9NKUHeOtSIn9JedrZVinFR6giZqD52A291GpF9BMrWV9jkxglG1ozQnqtOW/N3lIXBPXv9NQqfWP3UZd6jg5OcLaL4kwqHuMSnLGBEtVIgZ4lQtxuAFhB7dcJIfE6ZrFw0HyMT1hZrFGGJ/9ZkxxOo0i7L7yhqoReQQRjffRksPKby5fIXemxtjKfpDdc+wtGvx2JMQi61mCo0VD7Al+1I55hxxxvh7BC8/qckyfnL617QOdiXK9JQFHmwvj63Gxb7RztyiajOz8aOdaXvVmUEGYsXskwCOGVBc8TfZE09CFaYM2yX61btwQVYSQJ4uGrDaq9U18SmroXY7QQSuTVMI/1RQRk4zbrsYLp7It8dvoGHnaPy/eGKGYIkqNWWgGVuEY5fVJJtXaM2L9t8d6xVqlg6sf8PGN+L+RXrK7g==~-1~-1~1764867134~AAQAAAAE%2f%2f%2f%2f%2f2QDI8vQY88%2fPsUZbTNr2%2fbf+JmxaHXl0pseyDUDWTotnMyqXoACNiO6ARRmuWOfxgINQ8lRL8mBoRzLr+SD0H15PnxmARQm5B7OJ5cYtDXqVtytObCuX%2f1WVAGJmjsB7uBzLcnwc6uw4bVvE6ChLWpZqKxS0W2qtkCRMuX%2fiA%3d%3d~-1',
        'JSESSIONID': '0000nhGNpPzCnOJNuYpLp-1H89A:1c80pfoj2',
        'utag_main__sn': '11',
        'utag_main_ses_id': '1764865081675%3Bexp-session',
        'utag_main_dc_visit': '10',
        'utag_main_dc_region': 'eu-west-1%3Bexp-session',
        'utag_main__ss': '0%3Bexp-session',
        'tealium_data_action_lastEvent': 'click [MainNav Men][Men]',
        'tealium_data_action_lastAction': 'RayBanMetaGen1:RW4006:Pdp-Premium',
        'aka-cc': 'IN',
        'aka-ct': 'AHMEDABAD',
        'sgh-desktop-facet-state-plp': 'categoryid:undefined|gender:true|brands:partial|polarized:true|price:true|frame-shape:partial|color:true|face-shape:false|fit:false|materials:false|lens-treatment:false',
        'utag_main__pn': '11%3Bexp-session',
        'utag_main__se': '27%3Bexp-session',
        'utag_main__st': '1764867199539%3Bexp-session',
        'utag_main_dc_event': '11%3Bexp-session',
        '_scid_r': '39cnEDQ1MQje2bdZEMfrsARs45Ikp5Dd27_4Mw',
        '_uetsid': '38690260d01d11f08495fbd4a2bf905c',
        '_uetvid': '38697a60d01d11f0b79011998b46a49d',
        '_derived_epik': 'dj0yJnU9d0htZG5hM2VLMGdIVmZPb0xPS25kZ0VwNkY4bmltTnImbj1UZE1HVWJpaEkxLUpyM1A4UldRNDBBJm09NyZ0PUFBQUFBR2t4dFlBJnJtPTEmcnQ9QUFBQUFHZFIwSGsmc3A9Mg',
        'cto_bundle': 'Ov_XXF9WYyUyRmxETGVTR0glMkY2TWdIZXY1NmtRVWE5MWR6elVMSGJYdDF1WSUyRkFtbzFMQ0R5TEVjJTJGQ24zaWJTcm02S2VuWmFuMzhrWVFKblB0emYxYVBrcXVCZXNZRSUyRmFVcW52NGlPemc0RmRrT05RV01qQ09uZDdjbWd0ckNWUFd6Wld1RTNlcUZpJTJCb0lBOHFZWWo2dWdocmpOakFOR2VZSFpoSEx1NzMyJTJCRWhXUFo4VSUzRA',
        'bm_lso': '4B498C7D8D503B92EEE38DAC5FBF4DB413EB620618749014CFB73000622099A6~YAAQpowsMXNQv6GaAQAAyZ4s6gXUs4Ga2CwjCFDOjWKzk1J6pKmNlU5QNXUoFPqncCdMPHDLq51sCTLK+FkBRSxtMyMTdZK5jjz8aL3dhDsTcWcx8EcvDJHeU1MIiSjINi1r89YyBICaVd3jIKfNupH1iHyw47KNSiao2uhSqVn/4PW3AI9IklwWfHqoNcliL4QAPnDRzfhN49RXTLhb5DoaaWX8Ox7P3RL1Zz7V+jBSL0CL7H0/py7Co+i1kq7E76RNDI/D1AiYiZox3G0pCSTXXWkkXPUqht6xzFaxkR3nHHEK+FGgbXGuPcGj/EGWVicHHeskALGFvcKJZpZSPgZgYYfAADn4MsTeoz9fwbDQeKj8apFOPUhORvQeNow7IdWqcW4nmx6Qgk8DJab1v0IzxtVdj4YRR7PrIvWMIXaxXlobKp5Ydf1B+sg9YEzNHLQfFTXQKC6wXBG5KixggoqEQTXPQ2utFSYwTCK+n3g1a87kZEyT6vw=^1764865425197',
        'bm_s': 'YAAQnowsMdnslauaAQAAc5Ey6gQ+rmataEs+8dzTpA76GvzkR3LaKIm9MdggLemcgsHfOx8ZDSpLdpQqExm+X5wRTUWk3n/LtfoZiUxEmdfaI/NYXNG61DSLNPb1YiNLbgRJ0HRzawK/dekS7+fflc1EwTXRtpaJMelxsR8WGL5pOL1NcpHmOmFN5A+gVI0XrseEMHmYlzMzTn9Tqjjc1UcLx1jIuCdLRtW2iWGuMwKJH/LEAlHtlLq/gwqwoTTwSRjremhDKUk0IbzRH5KzvuPmBcK+TaknTw/39TJGnzLTcEefkn9ckbLtQImvAtR/N/9zNbkSuV8wrhFDXBS7uyt4++Lvznkd0F/hiQIy8mj2tiP6fKQThwZ/Lv0PZBkQGAzKTvS8pQ3UxwRdUtxeBrVjr3iDJudIP2NlB6Toidh9LvHCudwYAD98S6VUbC3ABLcS6DpgXK75n2HXj/SFqwTtSIGyXzJOr20KcPf33NQo+HfMA8xM6U4CSCqaBKA2AarAbJLeC/Dc2TZW94siN1HXvryatryaaMXh2MTkuLrTu4OtHsg5JYf+nvNTWQKsyO/qUclGbPQI1kwDtOFHwAkwbskpIAdzWRgzgsGA0W1qSi2+mJx9F5aXxb8283WimLwrcavHojWZnlfDiw8gy2I=',
        'bm_so': '456A2897B3362CF6D60CC2917F83FE83A8FEE6D748B3076519B66F247A973B9D~YAAQnowsMdrslauaAQAAc5Ey6gWdUbWnVlGFmu5CF7na5TMQ35/RgnWcYBtp4PjFOg0WgAwJVdKbANJtpc4hIMMpppeo5ZagYTNMI2NaA2ZAi0Eh4i1PH/FY7Fbk+PS1mpBhds5D8lH+ec0Z0xCqGagOQ8eizgsikgzN5/cnJzofGNUqd4iv0EsY513YkcvGZiFv9EceVNYygnj3EW0Bj5DsYFi9KwiD0yayPkrnK0bWoV39Vd31T5eqpQz6qMTgMIC3MhEnQ2SSzHBFcX/cvDAwRmiUhel2hG2w9DgzAjHiwGOMTkWCgiiNkUC9nTKq1JJUee6aDiCGR7z8ORQKrGViySjGtYFK83J06iFQaFlxSBw+iJ7hcpD7DR9YfVngGpRC8CtI9kmN5oYehylDljKZXxMyCZfSW7vFfO6AKsorgEM5c2gO4kJ0RNHqvKIFW8qgGAtMpjmuP2ydW3Bl9wjotvAbIFlNW3ZyHYaS4dwzGhrt4aSLGaA=',
        'bm_sz': 'E67814484DDEBD87B37757D20E534A76~YAAQnowsMdzslauaAQAAc5Ey6h5ox8HHHfA0SI8BGBI9xjfFdAhCQqhRbyVnomCOf1bQvbxQCZl72M1G85o5MdeMvKm3OPB+IKd8B27zKBckcFfWD+k0uZ53ZTuTntJ6aA8Bi0mQ2RWli63MdGHy2PhRQAc7nikMpTNPx50DEsnldwNbE6xl9DzJWF+ZcXxrkThuyr/tqoz2TxJOM9ivJrc8Ipk9slhUPscbc7hmSBTXU8qL+KNw+aYMsUNwnL+/fKqC6oy+jWRjksOPSUykjrc2+9ZfcmM8mfAS9klzwNDOOTjQOIK4boj/jgpSLgkzg+z5NJwJmmIoVU0wimG2UGehawiPvB+bHdWVl4EEibB+wgRdG/5YLyUl/8crxPTo/d4YhKBbmGHVhQDjEPalAUN5LxwgESXpFq7+eeYsi6omKrEoAjUpp3wG0SxMyS8=~3621943~3291189',
        '_ga_6P80B86QTY': 'GS2.1.s1764865085$o11$g1$t1764865773$j60$l0$h0',
        'TS011f624f': '015966d292a8ea37eef586d36d07549bc35a0d60a0aeb22c13cd85ef1555a939771bc391cd8f960b561ef1752715c84e014fa7e30fe28f02c828750169b62d7f076707381acae3489d0c11bd4e7777bfc812cd73c1',
        'bm_sv': '30C8C1EE54330CE5628CD3985519B491~YAAQnowsMWvtlauaAQAAAZcy6h5ZDZmRzO2uvMKfgunk3TBV4IkBuo7siGn2atZ1/NMzch5jSlXN/rMaJeuR9F9cSAHk77b53icn2QwobnXutRHzXS1vA1e99rmQzTfUyxVZRmgWZjAUyY/MawXg8+6zPZKGlkEeYJLNIlukXyVi4bb25YCcFUZWhqvbLoe4RE3kZMF2TNeFwjKIELYSqH3MP6Mpj16MxPh1ata7TybrU6KvWEglOWlyJZT6gDkOCnrAw/Lu~1',
        'RT': '"z=1&dm=sunglasshut.com&si=fcd43503-e2ac-463e-9077-85e9c28cec06&ss=mirn3uuz&sl=6&tt=2btl&bcn=%2F%2F684d0d4c.akstat.io%2F&obo=1"',
        'forterToken': 'fd970e168e6046b0abc33830f7980d2f_1764865775109__UDF4_6',
    }

    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8,hi;q=0.7,gu;q=0.6',
        'priority': 'u=1, i',
        'referer': f'{url}',
        'sec-ch-ua': '"Google Chrome";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
        # 'cookie': '_ALGOLIA=anonymous-46f23d88-cac9-4bf5-93a7-c549d3331301; mt.v=2.105080933.1764748038763; ftr_ncd=6; utag_main_v_id=019ae32e2c3c00902b12a3ce34400506f002106700978; utag_main_vapi_domain=sunglasshut.com; s_fid=0B3C78C6766F992F-0E24EE9853C27148; CONSENTMGR=consent:true%7Cts:1764748427868; _gcl_au=1.1.1231686658.1764748431; _ga=GA1.1.1228085581.1764748432; cjConsent=MHxOfDB8Tnww; cjUser=ea243434-6029-4f17-a6ad-49e8e50d3222; _scid=vlcnEDQ1MQje2bdZEMfrsARs45Ikp5Dd; _fbp=fb.1.1764748436637.468890404817721063; _pin_unauth=dWlkPVlUVmhOamN6WXpndE1Ua3pOUzAwT1RrekxUbGpNR0l0TW1KaE5UVTVZalkzTW1abQ; _ScCbts=%5B%5D; _sctr=1%7C1764700200000; SGPF=3DEh7Jdcw0B-QxFAgiZcZcPsaC1fQza1vLGTR5eIma7LE5gw5taGiaQ; sgh-desktop-facet-state-search=; tealium_data_tags_adobeAnalytics_trafficSourceMid_thisSession=direct; tiktok_click_id=undefined; s_cc=true; TrafficSource_Override=1; __idcontext=eyJjb29raWVJRCI6IjM2S2VFVW96akRtWURnSXdRRHV3WkFPNERGeSIsImRldmljZUlEIjoiMzZLZUVUcUJ5aUU0dm1jcmo2MEpYakhCV2xnIiwiaXYiOiIiLCJ2IjoiIn0%3D; _tt_enable_cookie=1; _ttp=01KBHXWBG5F7M4SXBQAZGTK7TY_.tt.1; ttcsid_CBL7BLBC77UFMFRPUAB0=1764823945431::usq109J82McdLW1raOz_.2.1764825378793.1; ttcsid=1764823945432::8LvwpSAvBNGaocrYk7ki.2.1764825378796.0; Lda_aKUr6BGRn=duertry.com/r/v2?; Lda_aKUr6BGRr=0; WC_PERSISTENT=Kq6qxAn4QTdvY5xjl0E8FU9c9Bm3UT4HlMKewR9U%2Ffk%3D%3B2025-12-04+09%3A36%3A44.629_1764841004629-399951_0; _cs_c=1; _cs_id=f7350921-c2c1-a137-bec3-8a8e18bc601d.1764841009.1.1764841009.1764841009.1.1799005009122.1.x; __pdst=c5921377b4b14c938def6defe98433ff; __rtbh.lid=%7B%22eventType%22%3A%22lid%22%2C%22id%22%3A%224vf477ahI0RZOpGOq9zA%22%2C%22expiryDate%22%3A%222026-12-04T09%3A36%3A52.290Z%22%7D; sm_uuid=1764841577955; Fm_kZf8ZQvmX=1; Ac_aqK8DtrDS=7; aka-zp=; bm_ss=ab8e18ef4e; ak_bmsc=B78A67C2091C7E5A2B3159531B5A77F9~000000000000000000000000000000~YAAQnowsMcqDlauaAQAA1fIn6h6vJgjRlXMjKaubBLgZzuFspjn+Km/OdTscSf7my0+O7/Eswej1oT9kC1McGV3zD8CXiAzAinDSl7xDbD0bN14X4IYc+sJ9PB0H3Uuqtgy8a7Lx2rzWTfNW8eNq9jYx28t5W5ahLZ5huimBlnmvu0mLm07I5p/yKrlm6Iotp0phc5+gautGyqAyOD02HE8GMQMxQUwKkJRttl0WQE4Dj4gnzh0qg8CcfAZXpYOWslBiXlUGollEEwR0qhOS6zSP3cdZ8zU9MgWr2MP7sZ3Glkvdp4W3HkalDkmFlmWRFI58tp8wZJMQ6xqihQw0a/z8rrWEOxTqlki+RV8am3271g5Wq9FeYVrtrwkj1JZvK4iZcgmZY1ospJeqVzPyl1U9RPNChyt/iizBOqOY+EM+9HS+Rgw=; _abck=26A3EEEA561EE583E6B9E9BEB82609A5~0~YAAQnowsMfqDlauaAQAAAfgn6g89vwPf8HmgYaQEWNz4xFYb1FYLZjkyqOlsCUVW3eHuKbSpX1vbq6ETY0jgmNls+hlzdwip5Z/LYyZn2MbKKmM9QEo4cnqSEkxccdoyEbx3ZNrn6hzMnWlQqFP+TRd2AIeU4PrzYrN/W1QfhFWY6ehQ9JzFqdfZZR2l3pdINf1BHxwib3rSDdIaQKax6C9/EKY9sAbmGJ51BX8tjdh0PxR7W4fBZZVJvtCXLIN13bMNYGbmsJf77o1zDAbrmy+Owzg1YbPt4NcuO9NKUHeOtSIn9JedrZVinFR6giZqD52A291GpF9BMrWV9jkxglG1ozQnqtOW/N3lIXBPXv9NQqfWP3UZd6jg5OcLaL4kwqHuMSnLGBEtVIgZ4lQtxuAFhB7dcJIfE6ZrFw0HyMT1hZrFGGJ/9ZkxxOo0i7L7yhqoReQQRjffRksPKby5fIXemxtjKfpDdc+wtGvx2JMQi61mCo0VD7Al+1I55hxxxvh7BC8/qckyfnL617QOdiXK9JQFHmwvj63Gxb7RztyiajOz8aOdaXvVmUEGYsXskwCOGVBc8TfZE09CFaYM2yX61btwQVYSQJ4uGrDaq9U18SmroXY7QQSuTVMI/1RQRk4zbrsYLp7It8dvoGHnaPy/eGKGYIkqNWWgGVuEY5fVJJtXaM2L9t8d6xVqlg6sf8PGN+L+RXrK7g==~-1~-1~1764867134~AAQAAAAE%2f%2f%2f%2f%2f2QDI8vQY88%2fPsUZbTNr2%2fbf+JmxaHXl0pseyDUDWTotnMyqXoACNiO6ARRmuWOfxgINQ8lRL8mBoRzLr+SD0H15PnxmARQm5B7OJ5cYtDXqVtytObCuX%2f1WVAGJmjsB7uBzLcnwc6uw4bVvE6ChLWpZqKxS0W2qtkCRMuX%2fiA%3d%3d~-1; JSESSIONID=0000nhGNpPzCnOJNuYpLp-1H89A:1c80pfoj2; utag_main__sn=11; utag_main_ses_id=1764865081675%3Bexp-session; utag_main_dc_visit=10; utag_main_dc_region=eu-west-1%3Bexp-session; utag_main__ss=0%3Bexp-session; tealium_data_action_lastEvent=click [MainNav Men][Men]; tealium_data_action_lastAction=RayBanMetaGen1:RW4006:Pdp-Premium; aka-cc=IN; aka-ct=AHMEDABAD; sgh-desktop-facet-state-plp=categoryid:undefined|gender:true|brands:partial|polarized:true|price:true|frame-shape:partial|color:true|face-shape:false|fit:false|materials:false|lens-treatment:false; utag_main__pn=11%3Bexp-session; utag_main__se=27%3Bexp-session; utag_main__st=1764867199539%3Bexp-session; utag_main_dc_event=11%3Bexp-session; _scid_r=39cnEDQ1MQje2bdZEMfrsARs45Ikp5Dd27_4Mw; _uetsid=38690260d01d11f08495fbd4a2bf905c; _uetvid=38697a60d01d11f0b79011998b46a49d; _derived_epik=dj0yJnU9d0htZG5hM2VLMGdIVmZPb0xPS25kZ0VwNkY4bmltTnImbj1UZE1HVWJpaEkxLUpyM1A4UldRNDBBJm09NyZ0PUFBQUFBR2t4dFlBJnJtPTEmcnQ9QUFBQUFHZFIwSGsmc3A9Mg; cto_bundle=Ov_XXF9WYyUyRmxETGVTR0glMkY2TWdIZXY1NmtRVWE5MWR6elVMSGJYdDF1WSUyRkFtbzFMQ0R5TEVjJTJGQ24zaWJTcm02S2VuWmFuMzhrWVFKblB0emYxYVBrcXVCZXNZRSUyRmFVcW52NGlPemc0RmRrT05RV01qQ09uZDdjbWd0ckNWUFd6Wld1RTNlcUZpJTJCb0lBOHFZWWo2dWdocmpOakFOR2VZSFpoSEx1NzMyJTJCRWhXUFo4VSUzRA; bm_lso=4B498C7D8D503B92EEE38DAC5FBF4DB413EB620618749014CFB73000622099A6~YAAQpowsMXNQv6GaAQAAyZ4s6gXUs4Ga2CwjCFDOjWKzk1J6pKmNlU5QNXUoFPqncCdMPHDLq51sCTLK+FkBRSxtMyMTdZK5jjz8aL3dhDsTcWcx8EcvDJHeU1MIiSjINi1r89YyBICaVd3jIKfNupH1iHyw47KNSiao2uhSqVn/4PW3AI9IklwWfHqoNcliL4QAPnDRzfhN49RXTLhb5DoaaWX8Ox7P3RL1Zz7V+jBSL0CL7H0/py7Co+i1kq7E76RNDI/D1AiYiZox3G0pCSTXXWkkXPUqht6xzFaxkR3nHHEK+FGgbXGuPcGj/EGWVicHHeskALGFvcKJZpZSPgZgYYfAADn4MsTeoz9fwbDQeKj8apFOPUhORvQeNow7IdWqcW4nmx6Qgk8DJab1v0IzxtVdj4YRR7PrIvWMIXaxXlobKp5Ydf1B+sg9YEzNHLQfFTXQKC6wXBG5KixggoqEQTXPQ2utFSYwTCK+n3g1a87kZEyT6vw=^1764865425197; bm_s=YAAQnowsMdnslauaAQAAc5Ey6gQ+rmataEs+8dzTpA76GvzkR3LaKIm9MdggLemcgsHfOx8ZDSpLdpQqExm+X5wRTUWk3n/LtfoZiUxEmdfaI/NYXNG61DSLNPb1YiNLbgRJ0HRzawK/dekS7+fflc1EwTXRtpaJMelxsR8WGL5pOL1NcpHmOmFN5A+gVI0XrseEMHmYlzMzTn9Tqjjc1UcLx1jIuCdLRtW2iWGuMwKJH/LEAlHtlLq/gwqwoTTwSRjremhDKUk0IbzRH5KzvuPmBcK+TaknTw/39TJGnzLTcEefkn9ckbLtQImvAtR/N/9zNbkSuV8wrhFDXBS7uyt4++Lvznkd0F/hiQIy8mj2tiP6fKQThwZ/Lv0PZBkQGAzKTvS8pQ3UxwRdUtxeBrVjr3iDJudIP2NlB6Toidh9LvHCudwYAD98S6VUbC3ABLcS6DpgXK75n2HXj/SFqwTtSIGyXzJOr20KcPf33NQo+HfMA8xM6U4CSCqaBKA2AarAbJLeC/Dc2TZW94siN1HXvryatryaaMXh2MTkuLrTu4OtHsg5JYf+nvNTWQKsyO/qUclGbPQI1kwDtOFHwAkwbskpIAdzWRgzgsGA0W1qSi2+mJx9F5aXxb8283WimLwrcavHojWZnlfDiw8gy2I=; bm_so=456A2897B3362CF6D60CC2917F83FE83A8FEE6D748B3076519B66F247A973B9D~YAAQnowsMdrslauaAQAAc5Ey6gWdUbWnVlGFmu5CF7na5TMQ35/RgnWcYBtp4PjFOg0WgAwJVdKbANJtpc4hIMMpppeo5ZagYTNMI2NaA2ZAi0Eh4i1PH/FY7Fbk+PS1mpBhds5D8lH+ec0Z0xCqGagOQ8eizgsikgzN5/cnJzofGNUqd4iv0EsY513YkcvGZiFv9EceVNYygnj3EW0Bj5DsYFi9KwiD0yayPkrnK0bWoV39Vd31T5eqpQz6qMTgMIC3MhEnQ2SSzHBFcX/cvDAwRmiUhel2hG2w9DgzAjHiwGOMTkWCgiiNkUC9nTKq1JJUee6aDiCGR7z8ORQKrGViySjGtYFK83J06iFQaFlxSBw+iJ7hcpD7DR9YfVngGpRC8CtI9kmN5oYehylDljKZXxMyCZfSW7vFfO6AKsorgEM5c2gO4kJ0RNHqvKIFW8qgGAtMpjmuP2ydW3Bl9wjotvAbIFlNW3ZyHYaS4dwzGhrt4aSLGaA=; bm_sz=E67814484DDEBD87B37757D20E534A76~YAAQnowsMdzslauaAQAAc5Ey6h5ox8HHHfA0SI8BGBI9xjfFdAhCQqhRbyVnomCOf1bQvbxQCZl72M1G85o5MdeMvKm3OPB+IKd8B27zKBckcFfWD+k0uZ53ZTuTntJ6aA8Bi0mQ2RWli63MdGHy2PhRQAc7nikMpTNPx50DEsnldwNbE6xl9DzJWF+ZcXxrkThuyr/tqoz2TxJOM9ivJrc8Ipk9slhUPscbc7hmSBTXU8qL+KNw+aYMsUNwnL+/fKqC6oy+jWRjksOPSUykjrc2+9ZfcmM8mfAS9klzwNDOOTjQOIK4boj/jgpSLgkzg+z5NJwJmmIoVU0wimG2UGehawiPvB+bHdWVl4EEibB+wgRdG/5YLyUl/8crxPTo/d4YhKBbmGHVhQDjEPalAUN5LxwgESXpFq7+eeYsi6omKrEoAjUpp3wG0SxMyS8=~3621943~3291189; _ga_6P80B86QTY=GS2.1.s1764865085$o11$g1$t1764865773$j60$l0$h0; TS011f624f=015966d292a8ea37eef586d36d07549bc35a0d60a0aeb22c13cd85ef1555a939771bc391cd8f960b561ef1752715c84e014fa7e30fe28f02c828750169b62d7f076707381acae3489d0c11bd4e7777bfc812cd73c1; bm_sv=30C8C1EE54330CE5628CD3985519B491~YAAQnowsMWvtlauaAQAAAZcy6h5ZDZmRzO2uvMKfgunk3TBV4IkBuo7siGn2atZ1/NMzch5jSlXN/rMaJeuR9F9cSAHk77b53icn2QwobnXutRHzXS1vA1e99rmQzTfUyxVZRmgWZjAUyY/MawXg8+6zPZKGlkEeYJLNIlukXyVi4bb25YCcFUZWhqvbLoe4RE3kZMF2TNeFwjKIELYSqH3MP6Mpj16MxPh1ata7TybrU6KvWEglOWlyJZT6gDkOCnrAw/Lu~1; RT="z=1&dm=sunglasshut.com&si=fcd43503-e2ac-463e-9077-85e9c28cec06&ss=mirn3uuz&sl=6&tt=2btl&bcn=%2F%2F684d0d4c.akstat.io%2F&obo=1"; forterToken=fd970e168e6046b0abc33830f7980d2f_1764865775109__UDF4_6',
    }

    params = {'langId': '-25',}

    response = requests.get(
        f'https://www.sunglasshut.com/wcs/resources/store/10154/products/{product_id}',
        params=params,
        cookies=cookies,
        headers=headers,
    )
    data = json.loads(response.text)
    
    # Detect the gender
    def detect_gender_from_text(text):
        if not text:
            return None
        
        t = text.lower()
        male_found = "men" in t or "male" in t
        female_found = "women" in t or "female" in t

        if male_found and female_found:
            return "unisex"
        if male_found:
            return "male"
        if female_found:
            return "female"
        
        return None

    categories = data.get("categories", {})
    final_categories = {}
    product_gender = None # not found any type of gender then Value is None

    for cat_id, cat_data in categories.items():
        name = cat_data.get("name", "")
        title = cat_data.get("title", "")
        g1 = detect_gender_from_text(name)
        g2 = detect_gender_from_text(title)
        if g1 and g2 and g1 != g2:
            gender = "unisex"
        else:
            gender = g1 or g2 or None

        cat_data["gender"] = gender
        final_categories[cat_id] = cat_data
        if not product_gender and gender: # product gender
            product_gender = gender
    
    size_val = data.get('analytics', {}).get('prodobj', {}).get('Size')
    fit_val = data.get('fit')

    feature = data.get('analytics', {}).get('prodobj', {}).get('LensTechnology')
    tech = data.get('analytics', {}).get('prodobj', {}).get('FrameTechnology')
    memoryCapacity = data.get('memoryCapacity')
    connectivity = data.get('connectivity')
    lensColorDetails = data.get('lensColorDetails')
    values = [feature, tech, memoryCapacity, connectivity, lensColorDetails]
    filtered = [str(v) for v in values if v]
    
    image_list = data.get("images", [])
    brandingLogoImagePath = data.get('brandingLogoImagePath')
    image_urls = " | ".join([img.get("url", "") for img in image_list if img.get("url")])
    if brandingLogoImagePath:
        image_urls = image_urls + " | " + brandingLogoImagePath if image_urls else brandingLogoImagePath
    
    model_full = data.get('analytics', {}).get('prodobj', {}).get('ModelName', None)
    model_first_word = model_full.split()[0] if model_full else None
    
    # Fetch all data
    data={
    "Product ID":product_id,
    "URL":url,
    "Genedr":product_gender,
    "UPC": data.get('insurance', {}).get('upc', None),
    "Brand": data.get('analytics', {}).get('prodobj', {}).get('Brand', None),
    "Title": (data.get('breadcrumb')[0].get('title')if data.get('breadcrumb') and len(data.get('breadcrumb')) > 0 else None),
    "Model": data.get('analytics', {}).get('prodobj', {}).get('ModelName', None),
    "Variations": data.get('links', {}).get('variants', None),
    "Polarized": data.get('polarized', None),
    "Price": data.get('insurance', {}).get('offerPrice', None),
    "Description": data.get('description', None),
    "Image URLs": image_urls,
    "Product Details": data.get('description', None),
    "Frame Color": data.get('analytics', {}).get('prodobj', {}).get('FrameColor', None),
    "Frame Material": data.get('frameMaterial', None),
    "Frame Shape": data.get('analytics', {}).get('prodobj', {}).get('Shape', None),
    "LensMaterial": data.get('lensMaterial', None),
    "Model Number": model_first_word,
    "Lens Color": data.get('analytics', {}).get('prodobj', {}).get('LensColor', None),
    "Size & Fit": f"{size_val} {fit_val}".strip() if (size_val or fit_val) else None,
    "Bridge Size":data.get('geoFit',None),
    "Lense Size":data.get('mostSoldSize',None),
    "Size":data.get('analytics', {}).get('prodobj', {}).get('Size',None),
    "Feature Technology":" | ".join(filtered) if filtered else None,
    }
    #print(data)
    
    try:
        pdp_table.insert_one(data)
        print(f"{count} : PDP Saved Successfully: {product_id}")

        pl_table.update_one(
            {"product_id": product_id},
            {"$set": {"status": "done"}}
        )

    except Exception as e:
        print(f"Error Saving PDP: {product_id}  |  Total Saved: {count}  |  Error: {e}")

count=0   
for item in pl_table.find({"status": "pending"}, {"url": 1, "product_id": 1}):
    url = item.get("url")
    product_id = item.get("product_id")
    count += 1
    print(f"Processing URL : {url} and Product ID : {product_id}")
    fetch_data(product_id, url, count)  # call the function of fetch_data

# export into csv
docs = list(pdp_table.find())

if docs:
    for doc in docs:
        for k, v in doc.items():
            if isinstance(v, int) and v > 10**15:  # very large integers
                doc[k] = str(v)

    # Write CSV
    with open("pdp_data.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=docs[0].keys())
        writer.writeheader()
        writer.writerows(docs)

    print("CSV file created successfully from pdp_table.")
else:
    print("No PDP data in table, CSV not created.")