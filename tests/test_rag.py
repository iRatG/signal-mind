"""Functional test for RAG system."""
import sys
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

from src.agent.rag import get_context, search_regulatory, search_corp, collection_stats

def sep(title):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print('='*60)

# 1. Collection stats
sep("Collection stats")
stats = collection_stats()
for name, count in stats.items():
    print(f"  {name}: {count} chunks")

# 2. Regulatory search — CBR docs
sep("CBR search: ключевая ставка инфляция 2022")
results = search_regulatory("ключевая ставка инфляция риски 2022", year=2022, top_k=3)
for r in results:
    m = r['meta']
    print(f"\n  [{m.get('type')} {m.get('year')} p.{m.get('page')}]  dist={r['distance']:.3f}")
    print(f"  {r['text'][:200]}")

# 3. Corp search — sanctions oil
sep("Corp search: санкции нефть экспорт 2022")
results = search_corp("санкции нефть экспорт ограничения 2022", year=2022, top_k=3)
for r in results:
    m = r['meta']
    print(f"\n  [{m.get('company')} {m.get('year')} p.{m.get('page')}]  dist={r['distance']:.3f}")
    print(f"  {r['text'][:200]}")

# 4. Full context block (what agent sees)
sep("Full context for agent: MOEXOG нефтяной сектор санкции")
ctx = get_context("MOEXOG нефтяной сектор санкции рубль 2022", year=2022, top_k=6)
if ctx:
    print(ctx[:1500])
else:
    print("  [no relevant context found]")

# 5. Sberbank specific
sep("Corp search: Сбербанк процентная ставка 2023")
results = search_corp("процентная ставка прибыль банк 2023", company="sberbank", year=2023, top_k=2)
for r in results:
    m = r['meta']
    print(f"\n  [{m.get('company')} {m.get('year')} p.{m.get('page')}]  dist={r['distance']:.3f}")
    print(f"  {r['text'][:200]}")

print("\n\nRAG functional test DONE")
