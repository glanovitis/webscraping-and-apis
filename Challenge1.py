import pandas as pd
import requests
from bs4 import BeautifulSoup


geography = """
<!DOCTYPE html>
<html>
<head> Geography</head>
<body>

<div class="city">
  <h2>London</h2>
  <p>London is the most popular tourist destination in the world.</p>
</div>

<div class="city">
  <h2>Paris</h2>
  <p>Paris was originally a Roman City called Lutetia.</p>
</div>

<div class="country">
  <h2>Spain</h2>
  <p>Spain produces 43,8% of all the world's Olive Oil.</p>
</div>

</body>
</html>
"""
# creating soup
soup = BeautifulSoup(geography, 'html.parser')
print(soup)

# finding all the fun facts
funfacts = soup.find_all("p")
print(funfacts)

# find all the names of all places
names = soup.find_all("h2")
print(names)

# all content of all cities (name and fact)
content_cities = soup.find_all(class_="city")
for content in content_cities:
    print(content.get_text())

# all names without facts of all cities
content_cities = soup.find_all(class_="city")
for content in content_cities:
    print(content.find("h2").get_text())
