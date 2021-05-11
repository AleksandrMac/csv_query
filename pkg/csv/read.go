package csv

import (
	"fmt"
	"strings"

	"go.uber.org/zap"
)

type Head struct {
	Path   string
	Fields []string
	Log    *zap.Logger
}

type Body struct {
	*Head
	Rows []Row
}

type Row struct {
	*Head
	Values []string
}

func (d *Row) IsMatch(match string) bool {
	if match == "" {
		return true
	}
	match = strings.TrimSpace(match)
	match = strings.ToUpper(match)

	lexInfix := GetLex(match)
	ReplaceFieldsToValues(lexInfix, d)
	lexPostfix := InfixToPostfix(lexInfix)
	result, err := GetBoolResult(lexPostfix)

	if err != nil {
		d.Log.Error(err.Error())
	}
	return result
}

//nolint
func GetBoolResult(postfix []string) (res bool, err error) {
	stack := make([]string, 0, len(postfix))
	for _, val := range postfix {
		l := len(stack)
		var str1, str2, result string
		if l > 0 {
			str1 = stack[l-1]
		}
		if l > 1 {
			str2 = stack[l-2]
		}

		switch val {
		case "!", "NOT":
			stack = stack[:l-2]
			stack = append(stack, str2)
			if result, err = not(str1); err != nil {
				return false, err
			}
			stack = append(stack, result)
		case "OR":
			stack = stack[:l-2]
			if result, err = or(str1, str2); err != nil {
				return false, err
			}
			stack = append(stack, result)
		case "AND":
			stack = stack[:l-2]
			if result, err = and(str1, str2); err != nil {
				return false, err
			}
			stack = append(stack, result)
		case "<":
			stack = stack[:l-2]
			if str1 > str2 {
				stack = append(stack, "1")
				continue
			}
			stack = append(stack, "0")
		case ">":
			stack = stack[:l-2]
			if str1 < str2 {
				stack = append(stack, "1")
				continue
			}
			stack = append(stack, "0")
		case ">=":
			stack = stack[:l-2]
			if str1 <= str2 {
				stack = append(stack, "1")
				continue
			}
			stack = append(stack, "0")
		case "<=":
			stack = stack[:l-2]
			if str1 >= str2 {
				stack = append(stack, "1")
				continue
			}
			stack = append(stack, "0")
		case "<>", "!=":
			stack = stack[:l-2]
			if str1 != str2 {
				stack = append(stack, "1")
				continue
			}
			stack = append(stack, "0")
		case "=":
			stack = stack[:l-2]
			if str1 == str2 {
				stack = append(stack, "1")
				continue
			}
			stack = append(stack, "0")
		default:
			stack = append(stack, val)
		}
	}
	if stack[0] == "1" {
		return true, nil
	}
	return false, nil
}

//nolint:goconst
func InfixToPostfix(infix []string) (postfix []string) {
	stack := make([]string, 0, len(infix))
	for _, val := range infix {
		val = strings.ToUpper(val)
		switch val {
		case "(":
			stack = append(stack, val)
		case ")":
			i := len(stack) - 1
			for ; stack[i] != "("; i-- {
				postfix = append(postfix, stack[i])
			}
			stack = stack[:i]
		case "+", "-", "!", "NOT", "/", "*", "DIV", "MOD", "AND", "OR", "<", ">", "<=", ">=", "<>", "!=", "=":
			for i := len(stack) - 1; i >= 0 && GetPriority(stack[i]) >= GetPriority(val); i = len(stack) - 1 {
				postfix = append(postfix, stack[i])
				stack = stack[:i]
			}
			stack = append(stack, val)
		default:
			postfix = append(postfix, val)
		}
	}
	for i := len(stack) - 1; i >= 0; i-- {
		postfix = append(postfix, stack[i])
	}

	return postfix
}

func ReplaceFieldsToValues(lex []string, row *Row) {
	for i, val := range lex {
		switch val {
		case "(", ")", "+", "-", "!", "NOT", "/", "*", "DIV", "MOD", "AND", "OR", "<", ">", "<=", ">=", "<>", "!=", "=":
			continue
		default:
			for j, field := range row.Fields {
				if val == field {
					lex[i] = "'" + row.Values[j] + "'"
					break
				}
			}
		}
	}
}

//nolint:gomnd
func GetPriority(operator string) uint8 {
	operator = strings.ToUpper(operator)
	switch operator {
	case "(", ")":
		return 1
	case "+", "-", "!", "NOT":
		return 2
	case "/", "*", "DIV", "MOD", "AND":
		return 3
	case "OR":
		return 4
	case "<", ">", "<=", ">=", "<>", "!=", "=":
		return 5
	default:
		return 0
	}
}
func GetLex(str string) (lex []string) {
	for left, right := "", str; len(right) > 0; {
		left, right = Split(right)
		lex = append(lex, left)
	}
	return lex
}

func Split(str string) (left, right string) {
	inputBuffer := []byte(str)
	outputBuffer := make([]byte, 0, len(inputBuffer))
	spaceIgnoring := false
	for i, val := range inputBuffer {
		switch val {
		case ')', '(':
			if len(outputBuffer) > 0 {
				return string(outputBuffer), string(inputBuffer[i:])
			}
			return string(val), string(inputBuffer[i+1:])
		case '>', '<', '!', '=':
			if len(outputBuffer) > 0 {
				return string(outputBuffer), string(inputBuffer[i:])
			}
			outputBuffer = append(outputBuffer, val)
			switch inputBuffer[i+1] {
			case '=':
				outputBuffer = append(outputBuffer, inputBuffer[i+1])
				return string(outputBuffer), string(inputBuffer[i+2:])
			default:
				return string(outputBuffer), string(inputBuffer[i+1:])
			}
		default:
			switch val {
			case ' ':
				if !spaceIgnoring {
					if len(outputBuffer) > 0 {
						return string(outputBuffer), string(inputBuffer[i+1:])
					}
					continue
				}
				outputBuffer = append(outputBuffer, ' ')
			case []byte("'")[0]:
				if !spaceIgnoring {
					spaceIgnoring = true
					outputBuffer = append(outputBuffer, val)
					continue
				}
				outputBuffer = append(outputBuffer, val)
				return string(outputBuffer), string(inputBuffer[i+1:])
			default:
				outputBuffer = append(outputBuffer, val)
			}
		}
	}
	return string(outputBuffer), string(inputBuffer)
}

func SplitReverse(str string) (left, right string) {
	inputBuffer := []byte(str)
	outputBuffer := make([]byte, 0, len(inputBuffer))
	spaceIgnoring := false
	for i := len(str); i > 0; i-- {
		val := inputBuffer[i-1]
		switch val {
		case ')', '(':
			if len(outputBuffer) > 0 {
				i++
				return string(inputBuffer[:i-1]), string(reverse(outputBuffer))
			}
			return string(inputBuffer[:i-1]), string(val)
		case '>', '<', '=':
			outputBuffer = append(outputBuffer, val)
			val2 := inputBuffer[i-2]
			switch val2 {
			case '<', '>', '!':
				outputBuffer = append(outputBuffer, val2)
				return string(inputBuffer[:i-2]), string(reverse(outputBuffer))
			default:
				return string(inputBuffer[:i-1]), string(outputBuffer)
			}
		default:
			switch val {
			case ' ':
				if !spaceIgnoring {
					if len(outputBuffer) > 0 {
						return string(inputBuffer[:i]), string(reverse(outputBuffer))
					}
					continue
				}
				outputBuffer = append(outputBuffer, ' ')
			case []byte("'")[0]:
				if !spaceIgnoring {
					spaceIgnoring = true
					outputBuffer = append(outputBuffer, val)
					continue
				}
				outputBuffer = append(outputBuffer, val)
				return string(inputBuffer[:i-1]), string(reverse(outputBuffer))
			default:
				outputBuffer = append(outputBuffer, val)
			}
		}
	}
	return string(inputBuffer), string(reverse(outputBuffer))
}

func reverse(str []byte) []byte {
	newStr := make([]byte, 0, len(str))
	for i := len(str); i > 0; i-- {
		newStr = append(newStr, str[i-1])
	}
	return newStr
}

func GetFields(row, sep string) []string {
	row = strings.ToUpper(row)
	if sep == "" {
		sep = ","
	}
	return strings.Split(row, sep)
}

func (h *Head) NewRow() *Row {
	return &Row{Head: h}
}

func not(str string) (string, error) {
	switch str {
	case "1":
		return "0", nil
	case "0":
		return "1", nil
	default:
		return "", fmt.Errorf("syntax error: %s is not bool(expected '0' or '1')", str)
	}
}

func or(str1, str2 string) (string, error) {
	if isBool(str1) && isBool(str2) {
		if str1 == "1" || str2 == "1" {
			return "1", nil
		}
		return "0", nil
	}
	return "", fmt.Errorf("syntax error: %s is not bool(expected '0' or '1')", str2)
}

func and(str1, str2 string) (string, error) {
	if isBool(str1) && isBool(str2) {
		if str1 == "1" && str2 == "1" {
			return "1", nil
		}
		return "0", nil
	}
	return "", fmt.Errorf("syntax error: %s is not bool(expected '0' or '1')", str2)
}

func isBool(str string) bool {
	switch str {
	case "1", "0":
		return true
	default:
		return false
	}
}
