def find_place_row_numbers(text: str, place: str) -> list[float] | None:
    """
    Busca la fila de 'Huelma' y extrae 11 números:
      [día_actual] [d1]...[d7] [total_7d] [total_mes] [total_hidro]

    FIX: ignora el código de estación tipo 'P63' o 'E01' para no capturar el 63/01 como dato.
    """
    t = normalize_text(text)
    lines = [ln.strip() for ln in t.split("\n") if ln.strip()]

    idx = None
    for i, ln in enumerate(lines):
        if re.search(rf"\b{re.escape(place)}\b", ln, flags=re.IGNORECASE):
            idx = i
            break
    if idx is None:
        return None

    # Concatena línea de la estación + líneas siguientes si parte la fila
    block = lines[idx]
    j = idx + 1
    while j < len(lines):
        # si detectamos inicio claro de otra estación, paramos
        if re.match(r"^[A-Z]\d{2}\b", lines[j]) or re.match(r"^P\d+\b", lines[j]):
            break
        # si la siguiente línea parece continuación (solo números/espacios), la añadimos
        block += " " + lines[j]
        j += 1

        # si ya hay suficientes números, podemos parar pronto
        # (pero cuidado con el código P63, que quitamos abajo)
        # seguimos hasta tener de sobra
        if len(re.findall(r"-?\d+(?:[.,]\d+)?", block)) >= 14:
            break

    # FIX: elimina el prefijo de estación tipo "P63" o "E01" SOLO al inicio del bloque
    block_clean = re.sub(r"^\s*(?:P\d+|[A-Z]\d{2})\b", "", block, flags=re.IGNORECASE).strip()

    nums = re.findall(r"-?\d+(?:[.,]\d+)?", block_clean)
    if len(nums) < 11:
        return None

    vals = [to_float(x) for x in nums[:11]]
    return vals
