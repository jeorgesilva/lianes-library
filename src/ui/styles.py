import streamlit as st

def apply_styles():
    st.markdown("""
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600&display=swap');
        
        html, body, [class*="css"] {
            font-family: 'Inter', sans-serif;
        }

        .book-card {
            background: rgba(255, 255, 255, 0.05);
            border: 1px solid rgba(255, 255, 255, 0.1);
            border-radius: 12px;
            padding: 20px;
            margin-bottom: 15px;
            transition: all 0.3s ease;
        }
        
        .book-card:hover {
            border-color: #8B5CF6;
            background: rgba(139, 92, 246, 0.05);
            transform: translateY(-2px);
        }

        .match-tag {
            background: #8B5CF6;
            color: white;
            padding: 2px 8px;
            border-radius: 6px;
            font-size: 0.75rem;
            font-weight: 600;
        }
                /* --- ESTILOS NETFLIX (Scroll Horizontal) --- */
        .netflix-row {
            display: flex;
            overflow-x: auto;
            gap: 20px;
            padding: 10px 0px 20px 0px;
            scroll-behavior: smooth;
        }
        
        /* Esconde a barra de rolagem para ficar limpo */
        .netflix-row::-webkit-scrollbar { display: none; }
        
        .netflix-card {
            flex: 0 0 auto; /* Não deixa encolher */
            width: 140px;
            display: flex;
            flex-direction: column;
            cursor: pointer;
            transition: transform 0.3s ease;
        }
        
        .netflix-card:hover {
            transform: scale(1.08); /* Efeito de zoom ao passar o mouse */
        }
        
        .netflix-cover {
            width: 140px;
            height: 210px;
            border-radius: 8px;
            object-fit: cover;
            box-shadow: 0 4px 10px rgba(0,0,0,0.5);
            background-color: #2D3748; /* Cor de fundo se não tiver capa */
            display: flex;
            align-items: center;
            justify-content: center;
            color: #A0AEC0;
            font-size: 0.8rem;
            text-align: center;
            border: 1px solid rgba(255,255,255,0.1);
        }
        
        .netflix-title {
            color: #E2E8F0;
            font-size: 0.85rem;
            font-weight: 600;
            margin-top: 8px;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis; /* Coloca "..." se o título for longo */
        }
        </style>
    """, unsafe_allow_html=True)