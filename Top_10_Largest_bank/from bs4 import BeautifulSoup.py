from bs4 import BeautifulSoup

html = "<div><p>First paragraph.</p><p>Second paragraph.</p></div>"
soup = BeautifulSoup(html, 'html.parser')

first_p = soup.find_all('p')
print(first_p)  # Output: "First paragraph."
