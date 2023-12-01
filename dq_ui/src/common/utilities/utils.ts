const LABELS: Record<string, string> = {
  "CSV": "CSV",
  "SQL_VALIDATOR": "SQL Validator"
};

export const formatString = (text: string): string => {
    if (!text) return text;
    if (text in LABELS) return LABELS[text];

    return text
      // Split the text by underscore
      .split('_')
      // Capitalize the first letter of each word and join with spaces
      .map(word => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase())
      .join(' ');
  }