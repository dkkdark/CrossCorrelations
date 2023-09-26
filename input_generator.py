import random


def generate():
    product_list = ['cheese', 'tea', 'milk', 'coffee', 'bread', 'cake', 'oil', 'cabbage', 'onion', 'garlic', 'nuts',
                    'meat', 'bananas', 'tomatoes', 'eggs', 'juice']

    r = random.randint(2, 7)
    zero = 0
    str = ''
    while zero < r:
        random.shuffle(product_list)
        random_element = product_list.pop()
        zero += 1
        str = str + ' ' + random_element
    return str


def generate_input(num):
    file = open('input.txt', 'w')
    for i in range(1, num):
        file.write(generate() + '\n')

    file.close()


