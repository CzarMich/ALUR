document.addEventListener('DOMContentLoaded', () => {
    loadFhirProfiles();

    // Tab functionality
    document.querySelectorAll('.tab-link').forEach(tab => {
        tab.addEventListener('click', function() {
            document.querySelectorAll('.tab-link').forEach(link => link.classList.remove('active'));
            document.querySelectorAll('.tab-content').forEach(content => content.classList.remove('active'));

            this.classList.add('active');
            document.getElementById(this.dataset.tab).classList.add('active');
        });
    });
});

async function loadFhirProfiles() {
    try {
        const response = await fetch('/fhir_profiles');
        const data = await response.json();
        const dropdown = document.getElementById('fhirProfileDropdown');
        data.profiles.forEach(profile => {
            const option = document.createElement('option');
            option.value = profile;
            option.textContent = profile;
            dropdown.appendChild(option);
        });
    } catch (error) {
        displayFeedback('Error loading FHIR profiles: ' + error.message, 'error');
    }
}

async function mapAqlToFhir() {
    const aqlQuery = document.getElementById('aqlQuery').value;
    const fhirProfile = document.getElementById('fhirProfileDropdown').value;
    const fieldMappings = {}; // Assuming this is generated dynamically

    if (!aqlQuery || !fhirProfile || !Object.keys(fieldMappings).length) {
        displayFeedback('Missing required fields', 'error');
        return;
    }

    const formData = new FormData();
    formData.append('aqlQuery', aqlQuery);
    formData.append('fhirProfile', fhirProfile);
    formData.append('fieldMappings', JSON.stringify(fieldMappings));

    try {
        const response = await fetch('/map', {
            method: 'POST',
            body: formData
        });

        const result = await response.json();

        if (response.ok) {
            displayFeedback('Mapping successful.', 'success');
        } else {
            displayFeedback(result.error || 'Error mapping data.', 'error');
        }
    } catch (error) {
        displayFeedback('Error mapping data: ' + error.message, 'error');
    }
}

function displayFeedback(message, type) {
    const feedbackElement = document.getElementById('mappingFeedback');
    feedbackElement.textContent = message;
    feedbackElement.className = type; // Add a class to style the feedback
}
