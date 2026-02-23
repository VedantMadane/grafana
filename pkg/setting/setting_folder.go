package setting

import (
	"gopkg.in/ini.v1"
)

const maxNestedFolderDepth = 7

type FolderSettings struct {
	// MaxNestedFolderDepth is the configured maximum nesting depth for folders.
	// Must be between 1 and maxNestedFolderDepth (7).
	MaxNestedFolderDepth int
}

func readFolderSettings(iniFile *ini.File) FolderSettings {
	s := FolderSettings{}

	folderSection := iniFile.Section("folder")
	s.MaxNestedFolderDepth = folderSection.Key("max_nested_folder_depth").MustInt(4)
	if s.MaxNestedFolderDepth > maxNestedFolderDepth {
		s.MaxNestedFolderDepth = maxNestedFolderDepth
	}
	if s.MaxNestedFolderDepth < 1 {
		s.MaxNestedFolderDepth = 1
	}
	return s
}
