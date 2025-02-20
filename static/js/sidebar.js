document.addEventListener('DOMContentLoaded', function() {
    const sectionHeaders = Array.from(document.querySelectorAll('.section-header'));
    const sidebar = document.querySelector('#sidebar');
    
    const existingButtons = sidebar.querySelectorAll('.report-scroll-btn');
    existingButtons.forEach(btn => btn.remove());
    
    sectionHeaders.sort((a, b) => {
        return a.getBoundingClientRect().top - b.getBoundingClientRect().top;
    });
    
    sectionHeaders.forEach(header => {
        const title = header.querySelector('.section-title');
        if (title) {
            const button = document.createElement('button');
            button.textContent = title.textContent;
            button.classList.add('report-scroll-btn');
            button.addEventListener('click', () => {
                header.scrollIntoView({ behavior: 'smooth' });
            });
            
            sidebar.appendChild(button);
        }
    });
});
