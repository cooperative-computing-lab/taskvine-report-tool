
function setIFrame(iframePath) {
    return new Promise((resolve, reject) => {
        const iframe = document.getElementById('content-frame');
        if (!iframe.src.endsWith('/' + iframePath)) {
            iframe.onload = () => {
                resolve();
                iframe.onload = null;
            };
            iframe.onerror = (error) => {
                reject(error);
            };
            iframe.src = '/' + iframePath;
        } else {
            resolve();
        }
    });
}


// Display the current time on the header
function displayCurrentTime() {
    var now = new Date();
    var dateString = now.toLocaleString('en-US', {
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        hour12: false,
        timeZoneName: 'short'
    });
    document.getElementById("current-time").innerHTML = dateString;
}
// window.addEventListener('load', displayCurrentTime);

/*
// Hide or show the header when the button is clicked
document.getElementById('header-button').addEventListener('click', function() {
    var header = document.getElementById('header');
    var button = this;
    var isVisible = header.style.top === '0px' || header.style.top === '';

    var content = document.getElementById('content');
    if (isVisible) {
        header.style.top = '-130px'; // hide header
        header.style.borderBottom = 'none';
        button.style.clipPath = 'polygon(25% 40%, 50% 80%, 75% 40%)'; // change to triangle pointing down
        content.style.top = '0px';
        content.style.height = "100%"
    } else {
        header.style.top = '0px';    // show header
        header.style.borderBottom = '1px solid #ccc';
        button.style.clipPath = 'polygon(25% 60%, 50% 20%, 75% 60%)'; // change to triangle pointing up
        content.style.top = '140px';
        content.style.height = "calc(100% - 140px)"
    }
    content.offsetHeight;
});
*/



////////////////////////////////////////////////////////////////////////////////////
//  Click on the sidebar button to scroll to the corresponding section in the iframe
document.addEventListener('DOMContentLoaded', function() {
    var buttons = document.querySelectorAll('.report-scroll-btn');
    buttons.forEach(function(button) {
        button.addEventListener('click', async function() {
            await setIFrame('report');
            var targetId = this.getAttribute('data-target');
            navigateWithinIframe(targetId);
        });
    });
});

function navigateWithinIframe(targetId) {
    var iframe = document.getElementById('content-frame');
    if (iframe && iframe.contentWindow) {
        if (iframe.contentWindow.document.readyState === 'complete') {
            scrollIframeTo(iframe, targetId);
        } else {
            iframe.onload = function() {
                scrollIframeTo(iframe, targetId);
            };
        }
    }
}

function scrollIframeTo(iframe, targetId) {
    var target = iframe.contentWindow.document.querySelector(targetId);
    if (target) {
        target.scrollIntoView({ behavior: 'smooth' });
    }
}

// Show debug html if click the debug button
document.addEventListener('DOMContentLoaded', function() {
    // debug button
    document.getElementById('btn-debug').addEventListener('click', function() {
        setIFrame('debug');
    });
    // performance button
    document.getElementById('btn-performance').addEventListener('click', function() {
        setIFrame('performance');
    });
    // transactions button
    document.getElementById('btn-transactions').addEventListener('click', function() {
        setIFrame('transactions');
    });
});
////////////////////////////////////////////////////////////////////////////////////



////////////////////////////////////////////////////////////////////////////////////



////////////////////////////////////////////////////////////////////////////////////