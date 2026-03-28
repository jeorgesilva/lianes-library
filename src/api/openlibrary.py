import requests
from pyzbar.pyzbar import decode
from PIL import Image
import streamlit as st

def buscar_livro_openlibrary(isbn):
    """Busca os dados do livro usando a API gratuita da Open Library."""
    url = f"https://openlibrary.org/api/books?bibkeys=ISBN:{isbn}&format=json&jscmd=data"
    
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            key = f"ISBN:{isbn}"
            
            if key in data:
                book_info = data[key]
                title = book_info.get("title", "Título desconhecido")
                
                # Extrair autor (a API retorna uma lista de dicionários)
                authors_list = book_info.get("authors", [])
                author = authors_list[0]["name"] if authors_list else "Autor desconhecido"
                
                # Extrair link da capa (tamanho Large 'L')
                cover_url = book_info.get("cover", {}).get("large")
                
                return {
                    "title": title, 
                    "author": author, 
                    "isbn": isbn, 
                    "cover_url": cover_url
                }
    except Exception as e:
        st.error(f"Erro ao conectar com a Open Library: {e}")
        
    return None 