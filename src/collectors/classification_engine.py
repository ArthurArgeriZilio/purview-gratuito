import re
import logging
from typing import List, Dict, Any

class ClassificationEngine:
    """
    PII and sensitive data classification engine.
    Scans data samples and identifies sensitive information patterns.
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # Brazilian patterns
        self.patterns = {
            'CPF': {
                'regex': r'\b\d{3}\.?\d{3}\.?\d{3}-?\d{2}\b',
                'sensitivity': 'High',
                'category': 'National ID'
            },
            'CNPJ': {
                'regex': r'\b\d{2}\.?\d{3}\.?\d{3}/?\d{4}-?\d{2}\b',
                'sensitivity': 'Medium',
                'category': 'Business ID'
            },
            'Email': {
                'regex': r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
                'sensitivity': 'Medium',
                'category': 'Contact Info'
            },
            'Phone_BR': {
                'regex': r'\b(?:\+55\s?)?(?:\(?\d{2}\)?\s?)?(?:9\s?)?\d{4}-?\d{4}\b',
                'sensitivity': 'Low',
                'category': 'Contact Info'
            },
            'Credit_Card': {
                'regex': r'\b(?:\d{4}[\s-]?){3}\d{4}\b',
                'sensitivity': 'Critical',
                'category': 'Financial'
            },
            'IP_Address': {
                'regex': r'\b(?:\d{1,3}\.){3}\d{1,3}\b',
                'sensitivity': 'Low',
                'category': 'Technical'
            },
            'SSN_US': {
                'regex': r'\b\d{3}-\d{2}-\d{4}\b',
                'sensitivity': 'Critical',
                'category': 'National ID'
            },
            'Date_BR': {
                'regex': r'\b\d{2}/\d{2}/\d{4}\b',
                'sensitivity': 'Low',
                'category': 'Temporal'
            }
        }
    
    def scan_text_sample(self, text: str) -> List[Dict[str, Any]]:
        """
        Scan a text sample for PII/sensitive data patterns.
        
        Args:
            text: Text content to scan
        
        Returns:
            List of detected sensitive data types
        """
        if not text:
            return []
        
        findings = []
        
        for label, pattern_info in self.patterns.items():
            matches = re.findall(pattern_info['regex'], text)
            
            if matches:
                findings.append({
                    'label': label,
                    'category': pattern_info['category'],
                    'sensitivity': pattern_info['sensitivity'],
                    'occurrences': len(matches),
                    'sample': matches[0] if matches else None  # First match as sample
                })
        
        return findings
    
    def classify_column(self, column_name: str, sample_values: List[str]) -> Dict[str, Any]:
        """
        Classify a database column based on name and sample values.
        
        Args:
            column_name: Name of the column
            sample_values: List of sample values (up to 100)
        
        Returns:
            Classification result with confidence score
        """
        # Name-based classification
        name_lower = column_name.lower()
        name_classifications = []
        
        if any(kw in name_lower for kw in ['cpf', 'ssn', 'social', 'tax_id']):
            name_classifications.append({'label': 'CPF/SSN', 'confidence': 0.8})
        
        if any(kw in name_lower for kw in ['email', 'e-mail', 'mail']):
            name_classifications.append({'label': 'Email', 'confidence': 0.9})
        
        if any(kw in name_lower for kw in ['phone', 'telefone', 'celular', 'mobile']):
            name_classifications.append({'label': 'Phone', 'confidence': 0.8})
        
        if any(kw in name_lower for kw in ['card', 'credit', 'cartao']):
            name_classifications.append({'label': 'Credit Card', 'confidence': 0.7})
        
        # Value-based classification
        combined_text = ' '.join(str(v) for v in sample_values[:100] if v)
        value_findings = self.scan_text_sample(combined_text)
        
        # Combine results
        if value_findings:
            # Value detection takes precedence
            top_finding = max(value_findings, key=lambda x: x['occurrences'])
            return {
                'column_name': column_name,
                'classification': top_finding['label'],
                'category': top_finding['category'],
                'sensitivity': top_finding['sensitivity'],
                'confidence': 0.95,
                'detection_method': 'Pattern Match',
                'sample_matches': top_finding['occurrences']
            }
        
        elif name_classifications:
            # Fall back to name-based
            top_name = max(name_classifications, key=lambda x: x['confidence'])
            return {
                'column_name': column_name,
                'classification': top_name['label'],
                'confidence': top_name['confidence'],
                'detection_method': 'Column Name',
                'sensitivity': 'Unknown'
            }
        
        else:
            # No classification
            return {
                'column_name': column_name,
                'classification': 'Unclassified',
                'confidence': 0.0,
                'sensitivity': 'Unknown'
            }
    
    def generate_classification_report(self, findings: List[Dict]) -> Dict:
        """
        Generate summary report from classification findings.
        
        Returns:
            Summary with sensitivity distribution
        """
        if not findings:
            return {
                'total_findings': 0,
                'critical': 0,
                'high': 0,
                'medium': 0,
                'low': 0,
                'categories': {}
            }
        
        sensitivity_counts = {
            'Critical': 0,
            'High': 0,
            'Medium': 0,
            'Low': 0
        }
        
        categories = {}
        
        for finding in findings:
            sens = finding.get('sensitivity', 'Unknown')
            if sens in sensitivity_counts:
                sensitivity_counts[sens] += 1
            
            cat = finding.get('category', 'Unknown')
            categories[cat] = categories.get(cat, 0) + 1
        
        return {
            'total_findings': len(findings),
            'critical': sensitivity_counts['Critical'],
            'high': sensitivity_counts['High'],
            'medium': sensitivity_counts['Medium'],
            'low': sensitivity_counts['Low'],
            'categories': categories
        }
