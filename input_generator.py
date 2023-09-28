import random
import numpy as np

def generate():
    product_list = ['milk', 'bread','cheese', 'tea', 'pear', 'coffee', 'apple', 'cake', 'oil', 'cabbage', 'onion', 'garlic', 'nuts',
                    'ham', 'bananas', 'tomatoes', 'eggs','juice', 'bacon', 'chicken', 'mutton', 'sausage', 'avocado',
                    'cucumber', 'pea', 'blackberry', 'grape', 'lemon', 'lime', 'melon', 'cream', 'kefir', 'yogurt', 'chocolate', 'cupcake', 'dessert', 'honey', 'jam', 'sugar', 'lemonade', 'muffin']

    lambda_param = 0.2
    r = random.randint(3, 15)
    zero = 0
    product_list_demo = []
    while zero < r:
        random_value = np.random.exponential(scale=1 / lambda_param)
        index = int(random_value) % len(product_list)
        product_list_demo.append(product_list[index])
        zero += 1

    unique_set = set(product_list_demo)
    unique_list = list(unique_set)
    result_str = ' '.join(unique_list)
    return result_str


def generate_input(num):
    file = open('input.txt', 'w')
    for i in range(1, num):
        file.write(generate() + '\n')

    file.close()


