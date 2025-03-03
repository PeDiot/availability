import random, time
from selenium.webdriver import Chrome
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.webdriver import WebDriver


def init_webdriver(headless: bool = True) -> WebDriver:
    chrome_options = Options()
    chrome_options.add_argument("--disable-search-engine-choice-screen")
    chrome_options.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/91.0.4472.124 Safari/537.36"
    )

    # Required options for running in Docker
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")

    if headless:
        chrome_options.add_argument("--headless=new")

    driver = Chrome(options=chrome_options)
    driver.maximize_window()

    return driver


def gaussian_sleep(driver: WebDriver, mean: float = 1, std: float = 0.5) -> None:
    sleep_time = max(random.gauss(mean, std), 0)
    time.sleep(sleep_time)

    if driver:
        driver.execute_script("window.scrollBy(0, 100);")
