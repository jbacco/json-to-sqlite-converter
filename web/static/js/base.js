// Generates a URI with parameters.
let generateUri = function(uri, params) {
    return ''.concat(uri, '?', $.param(params));
};

// Fetches the current database name.
let getDatabase = function() {
    return $('#database').val();
};

$(document).ready(function () {
    // Create dropdowns.
    $('.ui.dropdown').dropdown();

    // Reload page when the database changes.
    $('#database').change(function () {
        window.location.href = generateUri(window.location.pathname, {database: getDatabase()});
    });
});
