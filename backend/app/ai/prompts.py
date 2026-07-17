class PromptBuilder:
    """
    Decoupled helper class to build dynamic prompts for AI translation runtimes.
    """
    @staticmethod
    def build_translation_prompt(source_language: str, target_language: str) -> str:
        """
        Generates the default translation prompt targeting direct output.
        """
        return (
            f"Translate this speech from {source_language} to {target_language}. "
            f"Only provide the direct translation without any explanation, intro, or conversational filler."
        )
