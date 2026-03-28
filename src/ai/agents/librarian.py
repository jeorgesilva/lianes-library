from src.ai.vectorstore import vibe_search
# Importing your exact function from the file you provided!
from src.ai.llm.hf_inference import simple_completion 

def chat_with_librarian(user_message: str) -> str:
    """
    RAG Agent (Mistral-7B): Searches for books in ChromaDB and formulates a response.
    """
    print("🔍 Searching for context in the vector database...")
    
    # 1. Retrieval: Search for relevant books based on the semantic "vibe"
    rag_results = vibe_search(user_message, limit=3)
    
    # 2. Context Formatting
    if not rag_results:
        context_text = "No books found in the library that match this search."
    else:
        # Extract titles, authors, and summaries from the search results
        context_text = "\n".join([
            f"- Title: {r['title']} | Author: {r['author']} | Summary: {r['content_summary']}" 
            for r in rag_results
        ])

    # 3. Prompt Creation (System Instructions + User Question)
    # Since Mistral Instruct responds very well to clear tags, we structure the prompt like this:
    prompt = f"""[INST] You are the friendly virtual assistant of "Liane's Library".
Your mission is to help users find books in Liane's personal collection.

Use ONLY the context provided below to answer the question. 
If the answer is not in the context, politely state that Liane doesn't have a book about that yet.
Be helpful, enthusiastic about reading, and concise (maximum of 3 paragraphs).

LIBRARY CONTEXT:
{context_text}

USER QUESTION:
{user_message} [/INST]"""

    print("🧠 Generating response with Mistral-7B via Hugging Face...")
    
    # 4. Response Generation using your hf_inference.py file
    # The API token will be automatically loaded from your .env by your function
    response_text = simple_completion(prompt=prompt)
    
    return response_text.strip()