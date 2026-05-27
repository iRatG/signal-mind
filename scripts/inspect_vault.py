import chromadb
client = chromadb.PersistentClient(path='db/chroma')
vault = client.get_collection('methodology')
print(f'Chunks: {vault.count()}')

all_data = vault.get(limit=vault.count(), include=['metadatas','documents'])
types, files, statuses = {}, {}, {}
for m in all_data['metadatas']:
    t = m.get('type','?')
    f = m.get('file','?')
    s = m.get('status','?')
    types[t]    = types.get(t,0) + 1
    files[f]    = files.get(f,0) + 1
    statuses[s] = statuses.get(s,0) + 1

print('\n--- Типы ---')
for k,v in sorted(types.items(), key=lambda x:-x[1]):
    print(f'  {k}: {v}')

print('\n--- Файлы ---')
for k,v in sorted(files.items(), key=lambda x:-x[1])[:25]:
    print(f'  {k}: {v}')

print('\n--- Статусы ---')
for k,v in sorted(statuses.items(), key=lambda x:-x[1]):
    print(f'  {k}: {v}')

print('\n--- Пример каждого типа ---')
seen = set()
for m, doc in zip(all_data['metadatas'], all_data['documents']):
    t = m.get('type','?')
    if t not in seen:
        seen.add(t)
        fname = m.get('file','')
        sect  = m.get('section','')
        stat  = m.get('status','')
        print(f'\n[type={t}] file={fname} section={sect} status={stat}')
        print(f'  {doc[:250]}')
