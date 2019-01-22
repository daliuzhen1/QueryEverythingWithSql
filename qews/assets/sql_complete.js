window.onload = function() {
  setTimeout(function(){
	CodeMirror.fromTextArea(document.getElementById('sql_statement_area'), {
	indentWithTabs: true,
	smartIndent: true,
	matchBrackets : true,
	extraKeys: {"Ctrl-Space": "autocomplete", "Tab":"autocomplete" },
	hintOptions: {tables: {
		users: ["name", "score", "birthDate"],
		countries: ["name", "population", "size"]
	}}
	}).on('change', editor => {
    console.log(editor.getValue());
	editorValue = editor.getValue();
	document.getElementById('sql_statement_value_from_mirror').value = editor.getValue();
	});
   }, 100);

};