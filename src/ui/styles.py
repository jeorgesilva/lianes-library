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
        /* --- ESTILOS NETFLIX PREMIUM (Dark Mode & Hover Overlay) --- */
        .nx-row {
            display: flex;
            overflow-x: auto;
            overflow-y: hidden;
            gap: 15px;
            /* Padding generoso para a imagem não ser cortada quando crescer */
            padding: 40px 10px 40px 10px; 
            scroll-behavior: smooth;
        }
        
        .nx-row::-webkit-scrollbar { display: none; } /* Esconde a scrollbar */
        
        .nx-card {
            position: relative;
            flex: 0 0 auto;
            width: 160px;
            height: 240px;
            border-radius: 8px;
            cursor: pointer;
            transition: transform 0.4s cubic-bezier(0.25, 0.46, 0.45, 0.94), z-index 0.4s;
            z-index: 1;
            background-color: #2D3748;
            box-shadow: 0 4px 10px rgba(0,0,0,0.3);
        }
        
        /* A Mágica do Hover: Cresce 20%, vem para a frente e ganha sombra profunda */
        .nx-card:hover {
            transform: scale(1.25);
            z-index: 10;
            box-shadow: 0 20px 40px rgba(0,0,0,0.8);
        }
        
        .nx-cover {
            width: 100%;
            height: 100%;
            border-radius: 8px;
            object-fit: cover;
        }

        /* Overlay Gradiente (Escondido por padrão) */
        .nx-overlay {
            position: absolute;
            bottom: 0;
            left: 0;
            right: 0;
            height: 65%; /* Ocupa a parte de baixo da capa */
            background: linear-gradient(to top, rgba(15, 15, 15, 0.95) 0%, rgba(15, 15, 15, 0.8) 40%, transparent 100%);
            border-radius: 0 0 8px 8px;
            opacity: 0;
            transition: opacity 0.4s ease;
            display: flex;
            flex-direction: column;
            justify-content: flex-end;
            padding: 12px;
            color: #FFFFFF;
        }

        .nx-card:hover .nx-overlay {
            opacity: 1; /* Aparece no Hover */
        }

        .nx-title {
            font-size: 0.85rem;
            font-weight: 700;
            margin-bottom: 4px;
            line-height: 1.1;
            text-shadow: 1px 1px 2px black;
        }

        .nx-synopsis {
            font-size: 0.65rem;
            color: #D1D5DB;
            line-height: 1.3;
            margin-bottom: 10px;
            /* Trunca o texto em 3 linhas */
            display: -webkit-box;
            -webkit-line-clamp: 3;
            -webkit-box-orient: vertical;
            overflow: hidden;
        }

        /* Barra de Ações (Botões minimalistas) */
        .nx-actions {
            display: flex;
            gap: 8px;
        }

        .nx-btn {
            background: rgba(255, 255, 255, 0.15);
            border: 1px solid rgba(255, 255, 255, 0.4);
            border-radius: 50%;
            width: 28px;
            height: 28px;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 0.8rem;
            color: white;
            transition: background 0.2s, transform 0.2s;
        }

        .nx-btn:hover {
            background: #FFFFFF;
            color: #000000;
            transform: scale(1.1);
        }
        </style>
    """, unsafe_allow_html=True)