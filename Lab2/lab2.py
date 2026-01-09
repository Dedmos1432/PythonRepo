import re
class MyTranslator:
    def __init__(self):
        self.patterns = [
            (r"\b(let|const)\b", ""),
            (r"function\s+(\w+)\s*\((.*?)\)\s*\{", r"def \1(\2):"),
            (r"\}", ""),
            (r";", ""),
            (r"console\.log\s*\((.*?)\)", r"print(\1)"),
        ]

    def translate_for(self, line):
        match = re.match(
            r"for\s*\(\s*(?:let\s+)?(\w+)\s*=\s*(\d+);\s*\1\s*<\s*(\d+);\s*\1\+\+\s*\)\s*\{",line)
        if match:
            var, start, end = match.groups()
            return f"for {var} in range({start}, {end}):"

        match = re.match(
            r"for\s*\(\s*(?:let\s+)?(\w+)\s*=\s*(\d+);\s*\1\s*<=\s*(\d+);\s*\1\+\+\s*\)\s*\{",line)
        if match:
            var, start, end = match.groups()
            return f"for {var} in range({start}, {int(end) + 1}):"

        return None

    def translate_line(self, line):
        stripped = line.strip()

        if stripped.startswith("for"):
            result = self.translate_for(stripped)
            if result:
                return result

        for pattern, repl in self.patterns:
            line = re.sub(pattern, repl, line)

        line = line.replace("{", ":")
        return line.strip("\n")

    def translate(self, js_code: str):
        lines = js_code.splitlines()
        py_lines = []
        indent = 0

        for line in lines:
            stripped = line.strip()
            if stripped == "}":
                indent = max(0, indent - 1)
                continue

            py_line = self.translate_line(stripped)

            header = py_line.lstrip()
            if re.match(r"(def|if|else|elif|for)\b", header):
                py_lines.append("    " * indent + header)
                if header.endswith(":"):
                    indent += 1
            else:
                py_lines.append("    " * indent + py_line)

        return "\n".join(py_lines)

    def translate_file(self, input_file):
        with open(input_file, "r", encoding="utf-8") as f:
            js_code = f.read()
        py_code = self.translate(js_code)
        output_file = "pucode.txt"
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(py_code)
        print(f"Записано в", output_file)
        return py_code


if __name__ == "__main__":
    translator = MyTranslator()
    python_code = translator.translate_file("JS.txt")

    print("Python код")
    print(python_code)
