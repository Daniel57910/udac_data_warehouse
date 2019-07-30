import ftfy
import re

stri = "S\ufffdPaulo, Brazil"

regex = r'([^\s\w]+)'

print(re.sub(regex, '', stri))
