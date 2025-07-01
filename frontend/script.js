document.addEventListener("DOMContentLoaded", () => {
    const analyzeBtn = document.getElementById("analyze-btn");
    const resultsContainer = document.getElementById("results-container");
    const progressContainer = document.getElementById("progress-container");
    const progressBar = document.getElementById("progress-bar");
    const statusMessage = document.getElementById("status-message");
    const projectSelect = document.getElementById("project-select");
    const progressDetails = document.getElementById("progress-details");

    let eventSource;
    let fullAnalysisContext = []; // Store the full context here

    // Fetch and display list of projects on page load
    async function fetchProjects() {
        try {
            const response = await fetch("http://localhost:8000/api/projects");
            if (!response.ok) {
                throw new Error(`Failed to fetch projects: ${response.statusText}`);
            }
            const projects = await response.json();
            
            projectSelect.innerHTML = '<option value="">Select a project</option>'; // Clear loading message
            
            if (projects.length > 0) {
                projects.forEach(project => {
                    const option = document.createElement('option');
                    option.value = project.project_id;
                    option.textContent = project.project_id;
                    projectSelect.appendChild(option);
                });
                projectSelect.disabled = false;
            } else {
                projectSelect.innerHTML = '<option value="">No projects found</option>';
            }
        } catch (error) {
            console.error("Error fetching projects:", error);
            projectSelect.innerHTML = `<option value="">Error loading projects</option>`;
        }
    }

    fetchProjects();

    projectSelect.addEventListener("change", () => {
        if (projectSelect.value) {
            analyzeBtn.disabled = false;
        } else {
            analyzeBtn.disabled = true;
        }
    });

    analyzeBtn.addEventListener("click", () => {
        const selectedProject = projectSelect.value;
        if (!selectedProject) {
            alert("Please select a project to analyze.");
            return;
        }

        // Reset and show progress
        resultsContainer.classList.add("hidden");
        progressContainer.classList.remove("hidden");
        progressBar.style.width = "0%";
        statusMessage.textContent = "Initializing...";
        progressDetails.innerHTML = ''; // Clear previous details
        analyzeBtn.disabled = true;
        projectSelect.disabled = true;

        // Start the analysis by connecting to the streaming endpoint
        eventSource = new EventSource(`http://localhost:8000/api/analyze?project_id=${selectedProject}`);

        eventSource.addEventListener("checkpoint", (event) => {
            const data = JSON.parse(event.data);
            const items = progressDetails.getElementsByTagName('li');
            
            // Mark the last item as complete
            if (items.length > 0) {
                const lastItem = items[items.length - 1];
                lastItem.classList.add('completed');
                const spinner = lastItem.querySelector('.spinner');
                if(spinner) spinner.innerHTML = '<i class="fas fa-check-circle"></i>';
            }

            // Add the new item
            const newItem = document.createElement('li');
            newItem.innerHTML = `<span class="spinner"><i class="fas fa-spinner fa-spin"></i></span> ${data.text}`;
            progressDetails.appendChild(newItem);
        });

        eventSource.addEventListener("update", (event) => {
            const data = JSON.parse(event.data);

            // Store the full analysis context when it's available
            if (data.full_environment_data) {
                fullAnalysisContext = data.full_environment_data;
            }

            // Update progress bar and status message
            progressBar.style.width = `${data.progress}%`;
            statusMessage.textContent = data.details;

            // Check if the process is complete
            if (data.status === "Complete") {
                // Clear previous results and build the new card layout
                resultsContainer.innerHTML = ''; 

                const report = data.report;

                // Health Score Card
                const scoreCardContent = `
                    <div class="gauge-container">
                        <canvas id="health-gauge"></canvas>
                        <div id="health-score-value"></div>
                    </div>
                `;
                const scoreCard = createReportCard(
                    'Overall Health Score',
                    'fas fa-heartbeat',
                    scoreCardContent
                );
                resultsContainer.appendChild(scoreCard);

                // Make results visible BEFORE initializing the gauge
                resultsContainer.classList.remove("hidden");

                // --- Initialize Gauge ---
                const gaugeTarget = document.getElementById('health-gauge');
                const scoreValueEl = document.getElementById('health-score-value');

                const gaugeOptions = {
                    angle: -0.2, // The span of the gauge arc
                    lineWidth: 0.2, // The line thickness
                    radiusScale: 0.9, // Relative radius
                    pointer: {
                        length: 0.5, // Relative to gauge radius
                        strokeWidth: 0.035, // The thickness
                        color: '#333333' // Fill color
                    },
                    staticZones: [
                       {strokeStyle: "#F03E3E", min: 0, max: 40},   // Red
                       {strokeStyle: "#FFDD00", min: 40, max: 70},  // Yellow
                       {strokeStyle: "#30B32D", min: 70, max: 100}  // Green
                    ],
                    limitMax: false,
                    limitMin: false,
                    highDpiSupport: true,
                };

                const gauge = new Gauge(gaugeTarget).setOptions(gaugeOptions);
                gauge.maxValue = 100;
                gauge.setMinValue(0);
                gauge.animationSpeed = 32; // animation speed
                gauge.set(report.health_score);

                scoreValueEl.textContent = `${report.health_score} / 100`;

                // --- Add Gauge Legend & Rating Description ---
                const gaugeContainer = scoreValueEl.parentElement;

                // 1. Restore the original legend
                const legend = document.createElement('div');
                legend.className = 'gauge-legend';
                legend.innerHTML = `
                    <span class="legend-item"><span class="legend-dot red"></span> 0-40 Poor</span>
                    <span class="legend-item"><span class="legend-dot yellow"></span> 40-70 Fair</span>
                    <span class="legend-item"><span class="legend-dot green"></span> 70-100 Good</span>
                `;
                gaugeContainer.appendChild(legend);

                // 2. Add the dynamic rating description
                const ratingDescription = document.createElement('div');
                ratingDescription.className = 'gauge-rating-description';
                
                let ratingText = '';
                const score = report.health_score;
                if (score <= 40) {
                    ratingText = 'This score is considered <strong>Poor</strong>. There are significant opportunities for improvement in your BigQuery environment.';
                } else if (score <= 70) {
                    ratingText = 'This score is considered <strong>Fair</strong>. The environment is partially optimized, but several key best practices are not being followed.';
                } else {
                    ratingText = 'This score is considered <strong>Good</strong>. Your environment shows strong adherence to best practices with minor room for improvement.';
                }
                ratingDescription.innerHTML = ratingText;
                gaugeContainer.appendChild(ratingDescription);
                // --- End Gauge UI ---

                // Key Findings Card
                const findingsContentWrapper = document.createElement('div');
                const findingsDescription = document.createElement('p');
                findingsDescription.className = 'report-card-description';
                findingsDescription.textContent = 'Here are the key observations from the analysis of your project, ordered by importance:';
                findingsContentWrapper.appendChild(findingsDescription);

                const findingsContainer = document.createElement('div');
                findingsContainer.className = 'accordion-container';
                if (Array.isArray(report.key_findings) && report.key_findings.length > 0) {
                    report.key_findings.forEach(finding => {
                        const item = document.createElement('div');
                        item.className = 'accordion-item';
                        if (finding.importance) {
                            item.classList.add(`importance-${finding.importance.toLowerCase()}`);
                        }

                        const button = document.createElement('button');
                        button.className = 'accordion-button';
                        
                        let importanceIconHtml = '';
                        switch (finding.importance?.toLowerCase()) {
                            case 'high':
                                importanceIconHtml = '<i class="importance-icon fas fa-exclamation-triangle"></i>';
                                break;
                            case 'medium':
                                importanceIconHtml = '<i class="importance-icon fas fa-info-circle"></i>';
                                break;
                            case 'low':
                                importanceIconHtml = '<i class="importance-icon fas fa-check-circle"></i>';
                                break;
                        }
                        
                        // Add a chevron icon for visual cue
                        button.innerHTML = `<div>${importanceIconHtml}<span>${finding.title}</span></div><i class="fas fa-chevron-down"></i>`;

                        const panel = document.createElement('div');
                        panel.className = 'accordion-panel';
                        
                        // Add an inner content div to fix the padding/animation bug
                        const panelContent = document.createElement('div');
                        panelContent.className = 'accordion-content';
                        panelContent.innerHTML = marked.parse(finding.details || '');
                        panel.appendChild(panelContent);

                        button.addEventListener('click', () => {
                            button.classList.toggle('active');
                            const icon = button.querySelector('i');
                            icon.classList.toggle('fa-chevron-down');
                            icon.classList.toggle('fa-chevron-up');
                            
                            if (panel.style.maxHeight) {
                                panel.style.maxHeight = null;
                            } else {
                                panel.style.maxHeight = panel.scrollHeight + "px";
                            }
                        });

                        item.appendChild(button);
                        item.appendChild(panel);
                        findingsContainer.appendChild(item);
                    });
                } else {
                    findingsContainer.innerHTML = '<p>No key findings were identified.</p>';
                }
                findingsContentWrapper.appendChild(findingsContainer);

                const findingsCard = createReportCard(
                    'Key Findings',
                    'fas fa-search',
                    findingsContentWrapper
                );
                resultsContainer.appendChild(findingsCard);

                // Recommendations Card
                const recsContentWrapper = document.createElement('div');
                const recsDescription = document.createElement('p');
                recsDescription.className = 'report-card-description';
                recsDescription.textContent = 'Based on the findings, here are concrete, actionable steps you can take to improve your setup:';
                recsContentWrapper.appendChild(recsDescription);
                
                const recommendationsContainer = document.createElement('div');
                recommendationsContainer.className = 'accordion-container';

                if (Array.isArray(report.recommendations) && report.recommendations.length > 0) {
                    report.recommendations.forEach(rec => {
                        const item = document.createElement('div');
                        item.className = 'accordion-item';
                        if (rec.priority) { // Use priority for recommendations
                            item.classList.add(`importance-${rec.priority.toLowerCase()}`);
                        }

                        const button = document.createElement('button');
                        button.className = 'accordion-button';

                        let priorityIconHtml = '';
                        switch (rec.priority?.toLowerCase()) {
                            case 'high':
                                priorityIconHtml = '<i class="importance-icon fas fa-exclamation-triangle"></i>';
                                break;
                            case 'medium':
                                priorityIconHtml = '<i class="importance-icon fas fa-info-circle"></i>';
                                break;
                            case 'low':
                                priorityIconHtml = '<i class="importance-icon fas fa-check-circle"></i>';
                                break;
                        }

                        button.innerHTML = `<div>${priorityIconHtml}<span>${rec.title}</span></div><i class="fas fa-chevron-down"></i>`;

                        const panel = document.createElement('div');
                        panel.className = 'accordion-panel';

                        const panelContent = document.createElement('div');
                        panelContent.className = 'accordion-content';
                        panelContent.innerHTML = marked.parse(rec.details || '');
                        
                        // Add Action Plan button and container
                        const actionContainer = document.createElement('div');
                        actionContainer.className = 'action-plan-container';
                        const actionButton = document.createElement('button');
                        actionButton.className = 'action-plan-button';
                        actionButton.innerHTML = '<i class="fas fa-bolt"></i> Generate Action Plan';
                        const actionResult = document.createElement('div');
                        actionResult.className = 'action-plan-result';

                        actionButton.addEventListener('click', async (e) => {
                            e.stopPropagation(); // Prevent accordion from closing
                            actionButton.disabled = true;
                            actionButton.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Generating...';
                            actionResult.style.display = 'block';
                            actionResult.innerHTML = '';
                            
                            try {
                                const response = await fetch('http://localhost:8000/api/generate_action_plan', {
                                    method: 'POST',
                                    headers: {'Content-Type': 'application/json'},
                                    body: JSON.stringify({
                                        recommendation: rec,
                                        analysis_context: fullAnalysisContext // Correctly pass the stored context
                                    })
                                });
                                if (!response.ok) {
                                    const err = await response.json();
                                    throw new Error(err.detail || 'Failed to generate action plan');
                                }
                                const data = await response.json();
                                
                                // Clear previous results
                                actionResult.innerHTML = '';

                                // Create a new inner accordion for the action plan
                                const innerAccordionItem = document.createElement('div');
                                innerAccordionItem.className = 'inner-accordion';

                                const innerButton = document.createElement('button');
                                innerButton.className = 'inner-accordion-button';
                                innerButton.innerHTML = '<span>View Generated Action Plan</span><i class="fas fa-chevron-down"></i>';
                                
                                const innerPanel = document.createElement('div');
                                innerPanel.className = 'inner-accordion-panel';
                                
                                // FIX: Wrap content in its own div to solve animation bug
                                const innerPanelContent = document.createElement('div');
                                innerPanelContent.className = 'inner-accordion-content';
                                innerPanelContent.innerHTML = marked.parse(data.action_plan || '');
                                innerPanel.appendChild(innerPanelContent);

                                // Add click listener for the inner accordion
                                innerButton.addEventListener('click', (e) => {
                                    e.stopPropagation();
                                    innerButton.classList.toggle('active');
                                    const icon = innerButton.querySelector('i');
                                    icon.classList.toggle('fa-chevron-down');
                                    icon.classList.toggle('fa-chevron-up');

                                    // This function will resize the parent accordion.
                                    const resizeParent = () => {
                                        // Set the parent's max-height to fit its new content.
                                        panel.style.maxHeight = panel.scrollHeight + "px";
                                        // Remove the listener so this function only runs once per animation.
                                        innerPanel.removeEventListener('transitionend', resizeParent);
                                    };
                                    // Listen for the end of the inner accordion's animation.
                                    innerPanel.addEventListener('transitionend', resizeParent);
                                    
                                    // Trigger the animation by setting the max-height.
                                    if (innerPanel.style.maxHeight) {
                                        innerPanel.style.maxHeight = null;
                                    } else {
                                        innerPanel.style.maxHeight = innerPanel.scrollHeight + "px";
                                    }
                                });

                                innerAccordionItem.appendChild(innerButton);
                                innerAccordionItem.appendChild(innerPanel);
                                actionResult.appendChild(innerAccordionItem);

                                // Add 'Copy' buttons to all code blocks inside the new panel
                                innerPanelContent.querySelectorAll('pre code').forEach((codeBlock) => {
                                    const preElement = codeBlock.parentElement;
                                    const copyButton = document.createElement('button');
                                    copyButton.className = 'copy-code-button';
                                    
                                    preElement.appendChild(copyButton);

                                    copyButton.addEventListener('click', () => {
                                        navigator.clipboard.writeText(codeBlock.textContent).then(() => {
                                            copyButton.classList.add('copied');
                                            setTimeout(() => {
                                                copyButton.classList.remove('copied');
                                            }, 2000);
                                        });
                                    });
                                });

                                // FIX: Recalculate panel height after adding new content
                                panel.style.maxHeight = panel.scrollHeight + "px";
                                
                            } catch (error) {
                                actionResult.innerHTML = `<p class="error">Error: ${error.message}</p>`;
                            } finally {
                                actionButton.disabled = false;
                                actionButton.innerHTML = '<i class="fas fa-bolt"></i> Generate Action Plan';
                            }
                        });

                        actionContainer.appendChild(actionButton);
                        actionContainer.appendChild(actionResult);
                        panelContent.appendChild(actionContainer);
                        panel.appendChild(panelContent);

                        button.addEventListener('click', () => {
                            button.classList.toggle('active');
                            const icon = button.querySelector('i.fa-chevron-down, i.fa-chevron-up');
                            if (icon) {
                                icon.classList.toggle('fa-chevron-down');
                                icon.classList.toggle('fa-chevron-up');
                            }
                            
                            if (panel.style.maxHeight) {
                                panel.style.maxHeight = null;
                            } else {
                                panel.style.maxHeight = panel.scrollHeight + "px";
                            }
                        });

                        item.appendChild(button);
                        item.appendChild(panel);
                        recommendationsContainer.appendChild(item);
                    });
                } else {
                    recommendationsContainer.innerHTML = '<p>No recommendations were generated.</p>';
                }
                recsContentWrapper.appendChild(recommendationsContainer);

                const recommendationsCard = createReportCard(
                    'Recommendations',
                    'fas fa-lightbulb',
                    recsContentWrapper
                );
                resultsContainer.appendChild(recommendationsCard);

                // Reading List Card
                const readingListContentWrapper = document.createElement('div');
                const readingListDescription = document.createElement('p');
                readingListDescription.className = 'report-card-description';
                readingListDescription.textContent = 'Based on the analysis, here are some recommended articles and documentation for further reading.';
                
                const readingListResult = document.createElement('div');
                readingListResult.className = 'reading-list-result';
                
                if (data.reading_list && data.reading_list.length > 0) {
                    const list = document.createElement('ul');
                    list.className = 'reading-list';
                    data.reading_list.forEach(item => {
                        const listItem = document.createElement('li');
                        listItem.innerHTML = `<a href="${item.url}" target="_blank" rel="noopener noreferrer">${item.url}</a><p>${item.summary}</p>`;
                        list.appendChild(listItem);
                    });
                    readingListResult.appendChild(list);
                } else {
                    readingListResult.innerHTML = '<p>No specific articles were recommended at this time.</p>';
                }

                readingListContentWrapper.appendChild(readingListDescription);
                readingListContentWrapper.appendChild(readingListResult);

                const readingListCard = createReportCard(
                    'Recommended Reading',
                    'fas fa-book',
                    readingListContentWrapper
                );
                resultsContainer.appendChild(readingListCard);

                statusMessage.textContent = "Analysis Complete!";
                progressBar.style.backgroundColor = "#28a745"; /* Green for success */
                eventSource.close();
                analyzeBtn.disabled = false;
                projectSelect.disabled = false;

                // Mark the final checkpoint as complete
                const finalItems = progressDetails.getElementsByTagName('li');
                if (finalItems.length > 0) {
                    const lastItem = finalItems[finalItems.length - 1];
                    lastItem.classList.add('completed');
                    const spinner = lastItem.querySelector('.spinner');
                    if(spinner) spinner.innerHTML = '<i class="fas fa-check-circle"></i>';
                }
            }
        });
        
        eventSource.addEventListener("error", (event) => {
            let data;
            try {
                data = JSON.parse(event.data);
                statusMessage.textContent = `Error: ${data.details}`;
            } catch (e) {
                statusMessage.textContent = "An unknown error occurred on the backend.";
            }
            progressBar.style.backgroundColor = "#ff6b6b"; // Error color (Coral Red)
            eventSource.close();
            analyzeBtn.disabled = false;
            projectSelect.disabled = false;
        });

        eventSource.onerror = (err) => {
            console.error("EventSource failed:", err);
            statusMessage.textContent = "Failed to connect to the backend. Please ensure it's running and try again.";
            progressBar.style.backgroundColor = "#ff6b6b"; // Error color (Coral Red)
            eventSource.close();
            analyzeBtn.disabled = false;
            projectSelect.disabled = false;
        };
    });

    function createReportCard(title, iconClass, content) {
        const card = document.createElement('div');
        card.className = 'report-card';

        const header = document.createElement('div');
        header.className = 'report-card-header';

        const icon = document.createElement('i');
        icon.className = iconClass;

        const titleEl = document.createElement('h3');
        titleEl.textContent = title;

        header.appendChild(icon);
        header.appendChild(titleEl);

        const contentEl = document.createElement('div');
        contentEl.className = 'report-card-content';
        
        // Accept either an HTML string or a DOM element
        if (typeof content === 'string') {
            contentEl.innerHTML = content;
        } else {
            contentEl.appendChild(content);
        }

        card.appendChild(header);
        card.appendChild(contentEl);

        return card;
    }
});