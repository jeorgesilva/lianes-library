import streamlit as st

def apply_styles():
    st.markdown("""
        <style>
        /* Container da Linha - Força horizontal e esconde scroll */
        .nx-row {
            display: flex !important;
            flex-direction: row !important;
            flex-wrap: nowrap !important;
            overflow-x: auto !important;
            gap: 15px !important;
            padding: 20px 0 !important;
            width: 100% !important;
            -ms-overflow-style: none; 
            scrollbar-width: none;
        }
        .nx-row::-webkit-scrollbar { display: none !important; }

        /* Card - Tamanho fixo e reduzido */
        .nx-card {
            position: relative !important;
            flex: 0 0 120px !important; /* Largura fixa de 120px */
            width: 120px !important;
            height: 180px !important;
            border-radius: 6px !important;
            cursor: pointer !important;
            transition: transform 0.3s ease !important;
            background: #1A202C !important;
            z-index: 1 !important;
            overflow: hidden !important;
        }

        .nx-card:hover {
            transform: scale(1.1) !important;
            z-index: 100 !important;
        }

        /* Imagem da Capa */
        .nx-cover {
            width: 100% !important;
            height: 100% !important;
            object-fit: cover !important;
            display: block !important;
        }

        /* Overlay - Escondido por padrão, aparece apenas no HOVER */
        .nx-overlay {
            position: absolute !important;
            top: 0 !important; left: 0 !important; right: 0 !important; bottom: 0 !important;
            background: rgba(0, 0, 0, 0.8) !important;
            display: flex !important;
            flex-direction: column !important;
            justify-content: center !important;
            align-items: center !important;
            opacity: 0 !important; /* TOTALMENTE INVISÍVEL */
            transition: opacity 0.3s ease !important;
            padding: 10px !important;
            text-align: center !important;
            pointer-events: none !important; /* Não interfere no clique */
        }

        .nx-card:hover .nx-overlay {
            opacity: 1 !important; /* REVELA NO MOUSE */
        }

        .nx-title { color: white !important; font-size: 10px !important; font-weight: bold !important; }
        .nx-btn { font-size: 12px !important; margin-top: 5px !important; }
        </style>
    """, unsafe_allow_html=True)