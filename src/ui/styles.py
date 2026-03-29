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
        /* --- CONTAINER DO CARROSSEL (A LINHA) --- */
        .nx-row {
            display: flex !important;
            flex-direction: row !important;
            flex-wrap: nowrap !important; /* FORÇA a linha única */
            overflow-x: auto !important;  /* Habilita o scroll lateral */
            gap: 15px;
            padding: 20px 5px 30px 5px;
            width: 100%;
        }

        .nx-row::-webkit-scrollbar { display: none; } /* Esconde a scrollbar */

        /* --- O CARD (30% menor que o anterior) --- */
        .nx-card {
            position: relative;
            flex: 0 0 130px !important; /* LARGURA FIXA DE 130px */
            width: 130px !important;
            height: 195px !important;    /* ALTURA PROPORCIONAL */
            border-radius: 6px;
            cursor: pointer;
            transition: transform 0.3s ease;
            z-index: 1;
            background: #1A202C;
        }

        .nx-card:hover {
            transform: scale(1.1); /* Zoom leve para não bugar a tela */
            z-index: 99;
        }

        .nx-cover {
            width: 100% !important;
            height: 100% !important;
            object-fit: cover;
            border-radius: 6px;
        }

        /* OVERLAY (Escondido por padrão, aparece no hover) */
        .nx-overlay {
            position: absolute;
            bottom: 0;
            left: 0;
            right: 0;
            height: 100%;
            background: rgba(0, 0, 0, 0.7);
            display: flex;
            flex-direction: column;
            justify-content: center;
            align-items: center;
            opacity: 0; /* ESCONDIDO */
            transition: opacity 0.3s ease;
            padding: 10px;
            text-align: center;
            border-radius: 6px;
        }

        .nx-card:hover .nx-overlay {
            opacity: 1; /* SÓ APARECE NO HOVER */
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