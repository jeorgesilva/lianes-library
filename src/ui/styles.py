import streamlit as st

def apply_styles():
    st.markdown("""
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600&display=swap');
        
        html, body, [class*="css"] {
            font-family: 'Inter', sans-serif;
        }

        /* ... Seus estilos anteriores (book-card, match-tag) permanecem iguais ... */

        /* --- ESTILOS NETFLIX PREMIUM (Ajustados para Row Horizontal) --- */
        .nx-row {
            display: flex;
            flex-direction: row;      /* Força direção em linha */
            flex-wrap: nowrap;        /* CRÍTICO: Impede que os livros desçam para a próxima linha */
            overflow-x: auto;         /* Ativa o scroll lateral quando necessário */
            overflow-y: hidden;       /* Esconde o scroll vertical */
            gap: 20px;                /* Espaço maior entre as capas */
            padding: 40px 10px;       /* Espaço para o efeito de escala (zoom) não cortar */
            scroll-behavior: smooth;
            width: 100%;
        }
        
        /* Esconde a barra de rolagem para um visual limpo */
        .nx-row::-webkit-scrollbar { display: none; } 
        .nx-row { -ms-overflow-style: none; scrollbar-width: none; }
        
        .nx-card {
            position: relative;
            flex: 0 0 auto;           /* CRÍTICO: Impede que o card encolha. Mantém o tamanho fixo. */
            width: 180px;             /* Largura um pouco maior para parecer com a imagem */
            height: 270px;            /* Mantém proporção 2:3 */
            border-radius: 4px;       /* Cantos levemente arredondados como na imagem */
            cursor: pointer;
            transition: transform 0.4s ease, z-index 0.4s;
            z-index: 1;
            background-color: #1A202C;
            box-shadow: 0 4px 15px rgba(0,0,0,0.5);
        }
        
        .nx-card:hover {
            transform: scale(1.15);   /* Zoom elegante */
            z-index: 10;
            box-shadow: 0 20px 40px rgba(0,0,0,0.9);
        }
        
        .nx-cover {
            width: 100%;
            height: 100%;
            border-radius: 4px;
            object-fit: cover;        /* Garante que a capa preencha o card sem distorcer */
            display: block;
        }

        /* Overlay Gradiente */
        .nx-overlay {
            position: absolute;
            bottom: 0;
            left: 0;
            right: 0;
            height: 70%; 
            background: linear-gradient(to top, rgba(0,0,0,1) 0%, rgba(0,0,0,0.8) 40%, transparent 100%);
            border-radius: 0 0 4px 4px;
            opacity: 0;
            transition: opacity 0.3s ease;
            display: flex;
            flex-direction: column;
            justify-content: flex-end;
            padding: 15px;
            color: #FFFFFF;
        }

        .nx-card:hover .nx-overlay {
            opacity: 1;
        }

        .nx-title {
            font-size: 0.9rem;
            font-weight: 700;
            margin-bottom: 5px;
            line-height: 1.2;
        }

        .nx-synopsis {
            font-size: 0.7rem;
            color: #CBD5E0;
            line-height: 1.4;
            margin-bottom: 12px;
            display: -webkit-box;
            -webkit-line-clamp: 3;
            -webkit-box-orient: vertical;
            overflow: hidden;
        }

        .nx-actions {
            display: flex;
            gap: 10px;
        }

        .nx-btn {
            background: rgba(255, 255, 255, 0.2);
            border: 1px solid rgba(255, 255, 255, 0.3);
            border-radius: 50%;
            width: 32px;
            height: 32px;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 0.9rem;
            transition: all 0.2s;
        }

        .nx-btn:hover {
            background: #FFFFFF;
            color: #000000;
            transform: scale(1.1);
        }
        </style>
    """, unsafe_allow_html=True)