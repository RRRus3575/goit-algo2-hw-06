import string
import itertools
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict
import matplotlib.pyplot as plt
import requests

def get_text(url):
    try:
        response = requests.get(url)
        response.raise_for_status()  
        return response.text
    except requests.RequestException as e:
        return None

# Функція для видалення знаків пунктуації
def preprocess_text(text):
    text = text.lower()
    text = text.translate(str.maketrans("", "", string.punctuation))
    return text

def map_function(word):
    return word, 1

def shuffle_function(mapped_values):
    shuffled = defaultdict(list)
    for key, value in mapped_values:
        shuffled[key].append(value)
    return shuffled.items()

def reduce_function(key_values):
    key, values = key_values
    return key, sum(values)

# Виконання MapReduce
def map_reduce(text):
    # Видалення знаків пунктуації
    text = preprocess_text(text)
    words = text.split()

    # Паралельний Мапінг
    with ThreadPoolExecutor() as executor:
        mapped_values = list(executor.map(map_function, words))

    # Крок 2: Shuffle
    shuffled_values = shuffle_function(mapped_values)

    # Паралельна Редукція
    with ThreadPoolExecutor() as executor:
        reduced_values = list(executor.map(reduce_function, shuffled_values))

    result = sorted(reduced_values, key=lambda x: x[1], reverse=True)

    return result


def visualize_top_words(result, quantity):
    result = list(itertools.islice(result, quantity))

    words, frequencies = zip(*result)

    plt.figure(figsize=(10, 6))
    plt.barh(words, frequencies, color="skyblue")  
    plt.xlabel("Frequency")
    plt.ylabel("Words")
    plt.title(f"Top {quantity} Most Frequent Words")
    plt.gca().invert_yaxis()  
    plt.show()


if __name__ == '__main__':
    # Вхідний текст для обробки
    url = "https://gutenberg.net.au/ebooks01/0100021.txt"
    text = get_text(url)
    if text:
        # Виконання MapReduce на вхідному тексті
        result = map_reduce(text)
        visualize_top_words(result, 15)     

    else:
        print("Помилка: Не вдалося отримати вхідний текст.")
