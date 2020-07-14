package gorm

func TransformTranslateErrToHandleInsertionError(translateErrFn func(error) error) func(error) error {
	return func(err error) error {
		if err2 := translateErrFn(err); err2 != err {
			return err2
		}
		return nil
	}
}
