package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"os"
	"path/filepath"
	"strings"
)

func main() {
	if len(os.Args) != 2 {
		panic(fmt.Errorf("expected 1 argument, but got: %v", os.Args[1:]))
	}
	fileSet := token.NewFileSet()
	currentDirectory, err := os.Getwd()
	panicOnErr(err)
	fileName := os.Getenv("GOFILE")
	fullSourcePath := filepath.Join(currentDirectory, fileName)
	file, err := os.Open(fullSourcePath)
	if err != nil {
		panic(err)
	}
	defer panicOnCloseError(file)

	parsedFile, err := parser.ParseFile(fileSet, fileName, file, parser.AllErrors)
	panicOnErr(err)

	visitor := &structVisitor{
		packageName: os.Getenv("GOPACKAGE"),
		typeName:    os.Args[1],
	}
	for _, declaration := range parsedFile.Decls {
		ast.Walk(visitor, declaration)
	}
	visitor.generateMapping(currentDirectory, fileName)
}

type structVisitor struct {
	packageName string
	typeName    string
	template    *structMappingTemplate
}

type structMappingTemplate struct {
	packageName string
	typeName    string
	mappings    []structFieldMappingTemplate
}

type mappingType int

const (
	id mappingType = iota + 1
	elementId
	labels
	relationType
	property
	properties
)

func convertMappingType(mapping string) mappingType {
	switch mapping {
	case "id":
		return id
	case "element_id":
		return elementId
	case "labels":
		return labels
	case "type":
		return relationType
	case "property":
		return property
	case "properties":
		return properties
	}
	return 0
}

type structFieldMappingTemplate struct {
	fieldName    string
	fieldType    string
	mappingType  mappingType
	propertyName string
}

func (s *structVisitor) Visit(node ast.Node) ast.Visitor {
	structDeclaration, ok := node.(*ast.GenDecl)
	if !ok {
		return nil
	}
	if structDeclaration.Tok != token.TYPE {
		return nil
	}
	typeDefinition := s.findStructTypeDef(structDeclaration)
	if typeDefinition == nil {
		return nil
	}
	fields := typeDefinition.Fields.List
	template := &structMappingTemplate{
		packageName: s.packageName,
		typeName:    s.typeName,
		mappings:    make([]structFieldMappingTemplate, 0, len(fields)),
	}
	for _, field := range fields {
		mapping := parseFieldNeo4jMapping(field.Tag.Value)
		if mapping == nil {
			continue
		}
		template.mappings = append(template.mappings, structFieldMappingTemplate{
			fieldName:    field.Names[0].Name,
			fieldType:    extractType(field.Type),
			mappingType:  convertMappingType(mapping["mapping_type"]),
			propertyName: mapping["name"],
		})
	}
	s.template = template
	return nil
}

func extractType(value ast.Expr) string {
	switch f := value.(type) {
	case *ast.Ident:
		return f.Name
	case *ast.ArrayType:
		return fmt.Sprintf("[]%s", extractType(f.Elt))
	case *ast.MapType:
		return fmt.Sprintf("map[%s]%s", extractType(f.Key), extractType(f.Value))
	}
	panic(fmt.Errorf("unsupported AST element: %T", value))
}

func (s *structVisitor) findStructTypeDef(structDeclaration *ast.GenDecl) *ast.StructType {
	for _, spec := range structDeclaration.Specs {
		typeSpec, ok := spec.(*ast.TypeSpec)
		if !ok {
			continue
		}
		if typeSpec.Name.Name != s.typeName {
			continue
		}
		structType, ok := typeSpec.Type.(*ast.StructType)
		if !ok {
			return nil
		}
		return structType
	}
	return nil
}

func (s *structVisitor) generateMapping(directory string, originalFileName string) {
	extensionPosition := strings.LastIndex(originalFileName, ".go")
	if extensionPosition < 0 {
		panic(fmt.Errorf("invalid file name, expected .go file: %v", originalFileName))
	}
	newFileName := fmt.Sprintf("%s_generated_mapping.go", originalFileName[:extensionPosition])
	outputFile, err := os.Create(filepath.Join(directory, newFileName))
	if err != nil {
		panic(err)
	}
	defer panicOnCloseError(outputFile)
	var code strings.Builder
	code.WriteString("// Code generated by neo4j-mapper-gen - DO NOT EDIT\n")
	code.WriteString("\n")
	code.WriteString(fmt.Sprintf("package %s\n", s.packageName))
	code.WriteString("\n")
	code.WriteString("import \"github.com/neo4j/neo4j-go-driver/v5/neo4j\"\n")
	code.WriteString("\n")
	code.WriteString(fmt.Sprintf("func Map%[1]sFromRecord(record *neo4j.Record) (*%[1]s, error) {\n", s.typeName))
	code.WriteString("\tvalue := record.Values[0]\n")
	code.WriteString(fmt.Sprintf("\tresult := &%s{}\n", s.typeName))
	// TODO: handle type assertion errors and the likes
	for _, mapping := range s.template.mappings {
		switch mapping.mappingType {
		case id:
			code.WriteString(fmt.Sprintf("\tresult.%s = value.(neo4j.Entity).GetId()\n",
				mapping.fieldName))
		case elementId:
			code.WriteString(fmt.Sprintf("\tresult.%s = value.(neo4j.Entity).GetElementId()\n",
				mapping.fieldName))
		case relationType:
			code.WriteString(fmt.Sprintf("\tresult.%s = value.(neo4j.Relationship).Type\n",
				mapping.fieldName))
		case labels:
			code.WriteString(fmt.Sprintf("\tresult.%s = value.(neo4j.Node).Labels\n",
				mapping.fieldName))
		case properties:
			code.WriteString(fmt.Sprintf("\tresult.%s = value.(neo4j.Entity).GetProperties()\n",
				mapping.fieldName))
		case property:
			code.WriteString(fmt.Sprintf("\tresult.%s = value.(neo4j.Entity).GetProperties()[\"%s\"].(%s)\n",
				mapping.fieldName,
				mapping.propertyName,
				mapping.fieldType))
		}
	}
	code.WriteString("\treturn result, nil\n")
	code.WriteString("}\n")
	_, err = outputFile.WriteString(code.String())
	panicOnErr(err)
}

const neo4jTagPrefix = "neo4j:"

func parseFieldNeo4jMapping(rawTag string) map[string]string {
	if rawTag == "" {
		return nil
	}
	tag := extractNeo4jMappingTag(rawTag)
	if tag == "" {
		return nil
	}
	specs := strings.Split(tag, ",")
	settings := make(map[string]string, len(specs))
	for _, spec := range specs {
		key, setting, _ := strings.Cut(spec, "=")
		settings[key] = setting
	}
	return settings
}

func extractNeo4jMappingTag(rawTag string) string {
	tags := strings.Split(rawTag, " ")
	for i, tag := range tags {
		if i == 0 {
			tag = tag[len("`"):]
		}
		if i == len(tags)-1 {
			tag = tag[:len(tag)-len("`")]
		}
		tagPrefix := tag[:len(neo4jTagPrefix)]
		if tagPrefix != neo4jTagPrefix {
			continue
		}
		start := len(neo4jTagPrefix) + len(`"`)
		end := len(tag) - len(`"`)
		return tag[start:end]
	}
	return ""
}

func panicOnCloseError(closer io.Closer) {
	err := closer.Close()
	panicOnErr(err)
}

func panicOnErr(err error) {
	if err != nil {
		panic(err)
	}
}

//func main() {
//	fmt.Printf("Running %s go on %s\n", os.Args[0], os.Getenv("GOFILE"))
//
//	cwd, err := os.Getwd()
//	if err != nil {
//		panic(err)
//	}
//	fmt.Printf("  cwd = %s\n", cwd)
//	fmt.Printf("  os.Args = %#v\n", os.Args)
//
//	for _, ev := range []string{"GOARCH", "GOOS", "GOFILE", "GOLINE", "GOPACKAGE", "DOLLAR"} {
//		fmt.Println("  ", ev, "=", os.Getenv(ev))
//	}
//}

//Running mapper go on user.go
//cwd = /Users/fbiville/workspace/neo4j-example-mapper-project/pkg/users
//os.Args = []string{"mapper", "User"}
//GOARCH = arm64
//GOOS = darwin
//GOFILE = user.go
//GOLINE = 5
//GOPACKAGE = users
//DOLLAR = $
