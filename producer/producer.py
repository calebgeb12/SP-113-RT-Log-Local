from kafka import KafkaProducer
import json
import random
import time
from datetime import datetime
from colorama import Fore, Style, init


producer = KafkaProducer(
    bootstrap_servers=['kafka:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

class AcessLog():
    def __init__(self):
        self.type = self.determine_type()
        self.IP = self.generate_IP()
        self.ident = "-"
        self.user = self.generate_user()
        self.timestamp = self.generate_timestamp()
        self.request_line, self.endpoint = self.generate_request()
        self.code, self.code_type = self.generate_status_code()
        self.status_code = self.code
        self.bytes_sent = self.generate_bytes()
        self.referrer = self.generate_referrer()
        self.user_agent = self.generate_user_agent()
        self.log = (
            f'{self.IP} {self.ident} {self.user} '
            f'[{self.timestamp}] "{self.request_line}" '
            f'{self.status_code} {self.bytes_sent} '
            f'"{self.referrer}" "{self.user_agent}"'
        )    

    months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

    endpoints = [
        "/",
        "/home",
        "/about",
        "/contact",
        "/login",
        "/signup",
        "/logout",
        "/dashboard",
        "/dashboard/settings",
        "/api/users",
        "/api/users/123",
        "/api/auth/login",
        "/api/products",
        "/api/products/987",
        "/search?q=laptop&page=2",
        "/cart",
        "/checkout",
        "/assets/css/main.css",
        "/assets/img/banner.jpg",
        "/static/js/app.bundle.js"
    ]

    referrers = [
        "https://www.google.com/",
        "https://www.bing.com/",
        "https://search.yahoo.com/",
        "https://duckduckgo.com/",
        "https://www.baidu.com/",
        "https://www.yandex.com/",
        "https://www.facebook.com/",
        "https://twitter.com/",
        "https://www.instagram.com/",
        "https://www.linkedin.com/",
        "https://www.reddit.com/",
        "https://www.tiktok.com/",
        "https://www.pinterest.com/",
        "https://news.google.com/",
        "https://medium.com/",
        "https://www.nytimes.com/",
        "https://www.bbc.com/",
        "https://www.hackernews.com/",
        "https://www.producthunt.com/",
        "https://example.com/",
        "https://example.com/blog/post1",
        "https://example.com/contact",
        "https://github.com/",
        "https://slack.com/",
        "https://mail.google.com/",
        "https://analytics.google.com/",
    ]

    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
        "(KHTML, like Gecko) Version/17.0 Safari/605.1.15",
        "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:120.0) Gecko/20100101 Firefox/120.0",        
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 "
        "(KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
        "Mozilla/5.0 (Linux; Android 14; Pixel 7 Pro) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
        "Mozilla/5.0 (Linux; Android 13; SM-G996U) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/119.0.6045.134 Mobile Safari/537.36",
        "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
        "Mozilla/5.0 (compatible; Bingbot/2.0; +http://www.bing.com/bingbot.htm)",
        "DuckDuckBot/1.1; (+http://duckduckgo.com/duckduckbot.html)",
        "Mozilla/5.0 (compatible; YandexBot/3.0; +http://yandex.com/bots)",
        "Mozilla/5.0 (compatible; Baiduspider/2.0; +http://www.baidu.com/search/spider.html)",
        "curl/8.4.0",
        "PostmanRuntime/7.36.0",
        "Wget/1.21.3 (linux-gnu)",
        "python-requests/2.31.0",
        "Java/11.0.22",
    ]


    def determine_type(self):
        chance = random.randint(1 ,20)
        if (chance == 1):
            return "common"
        return "combined"
    
    def generate_IP(self):
        return ".".join(str(random.randint(0, 255)) for _ in range(4))
    
    def generate_user(self):
        user_chance = random.randint(1, 20)
        user = "-"
        if (user_chance == 1):
            user_options = ["admin", "guest", "specified user"]
            user = user_options[random.randint(0, 2)]
        
        return user

    def generate_timestamp(self):
        now = datetime.now()
        day = f"{now.day:02}"
        month = now.strftime("%b") 
        year = str(now.year)
        hour = f"{now.hour:02}"
        minute = f"{now.minute:02}"
        second = f"{now.second:02}"
        timestamp = "/".join([day, month, year, hour, minute, second])
        return timestamp
    
    def generate_request(self):
        methods = ["GET", "POST", "PUT", "DELETE"]
        method = methods[random.randint(0, len(methods) - 1)]
        endpoint = self.endpoints[random.randint(0, len(self.endpoints) - 1)]
        protocol = "HTTP/2.0"
        request = f"{method} {endpoint} {protocol}"
        return (request, endpoint)
    
    def generate_status_code(self):
        success = [200, 201, 204]
        redirects = [301, 302, 307]
        # client_errors = [400, 401, 403, 404]
        # server_errors = [500, 502, 503]
        client_errors = [404]
        server_errors = [500]

        error_chance = 20                            # Percentage 

        roll = random.randint(1, 100)
        if roll <= error_chance:
            category = random.choice([client_errors, server_errors])
        else:
            category = random.choice([success, redirects])

        code = random.choice(category)

        code_type = ""
        if category is success:
            code_type = "success"
        elif category is redirects:
            code_type = "redirect"
        elif category is client_errors:
            code_type = "client_error"
        elif category is server_errors:
            code_type = "server_error"
        
        return code, code_type

    def generate_bytes(self):
        range = random.randint(1, 100)
        bytes = 0
        if range < 90:
            bytes = random.randint(200, 10000)
        elif range < 98:
            bytes = random.randint(10000, 500000)
        else:
            bytes = random.randint(500000, 1000000)

        return str(bytes)

    def generate_referrer(self):
        chance = random.randint(1, 10)
        if chance > 4:
            return "-"
        
        return self.referrers[random.randint(0, len(self.referrers) - 1)]
    
    def generate_user_agent(self):
        return self.user_agents[random.randint(0, len(self.user_agents) - 1)]
    

class ErrorLog():
    def __init__(self, access_log, endpoint): 
        error_dict = {
            400: ["BadRequest", "Missing required field 'email'"],
            401: ["Unauthorized", "Invalid authentication token"],
            403: ["Forbidden", "User does not have permission to access this resource"],
            404: ["NotFound", "Requested resource could not be found"],

            500: ["InternalServerError", "Database connection timed out"],
            502: ["BadGateway", "Invalid response from upstream server"],
            503: ["ServiceUnavailable", "Server temporarily overloaded or under maintenance"]
        }

        self.IP = access_log.IP
        self.user = access_log.user
        self.timestamp = access_log.timestamp
        self.status_code = access_log.status_code
        self.referrer = access_log.referrer
        self.user_agent = access_log.user_agent
        self.error_type = error_dict[access_log.status_code][0]
        self.error_message = error_dict[access_log.status_code][1]
        self.endpoint = endpoint
        
        self.log = (
            f'{self.IP} {self.user} [{self.timestamp}] '
            f'{self.status_code} '
            f'"{self.referrer}" "{self.user_agent}" '
            f'"{self.error_type}: {self.error_message}" '
            f'"{self.endpoint}"'
        )


def generate_log():
    web_log = AcessLog()
    producer.send("web-logs", value=web_log.log)
    print(Fore.WHITE + web_log.log)
    if web_log.code_type == "redirect":
        redirect_log = AcessLog()
        producer.send("web-logs", value=redirect_log.log)
        print(Fore.GREEN + redirect_log.log)
    elif web_log.code_type in ["client_error", "server_error"]:
        error_log = ErrorLog(web_log, web_log.endpoint)
        producer.send("web-logs", value=error_log.log)
        print(Fore.RED + error_log.log)
    else:
        print(Fore.WHITE + web_log.log)
    

web_log_count = 10000
i = 0
# while i < web_log_count:
while True:
    # if random.random() < 0.3: 
    if random.random() < 0.8: 
        burst_length = random.randint(5, 15) 
        for _ in range(burst_length):
            generate_log()
            # time.sleep(random.uniform(0.05, 0.1))  
            time.sleep(random.uniform(0.01, 0.05))  
    else:
        generate_log()
        time.sleep(random.uniform(.2, .4))  
    i += 1

        




    
